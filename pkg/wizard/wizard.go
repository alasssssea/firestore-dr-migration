// Package wizard provides an interactive command-line flow that discovers a
// source MongoDB, lets the operator pick databases/collections and target
// Firestore endpoints, and writes a ready-to-run migration config file — so
// nobody has to hand-write the JSON (DESIGN §6).
package wizard

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"firestore-dr-migration/pkg/config"
	"firestore-dr-migration/pkg/db"
	"firestore-dr-migration/pkg/logger"
	"firestore-dr-migration/pkg/migration"
	"firestore-dr-migration/pkg/util"
	"go.mongodb.org/mongo-driver/bson"
)

// Selection captures the operator's choice for one source database.
type Selection struct {
	SourceDB       string
	TargetDB       string
	TargetConn     string
	AllCollections bool     // migrate the whole database
	Collections    []string // specific collections (ignored when AllCollections)
	// CollectionTuning holds optional per-collection partition-knob overrides,
	// keyed by source collection name. Independent of Collections, so it applies
	// in whole-database mode too.
	CollectionTuning map[string]config.CollectionTuning
}

// BuildConfig turns wizard selections into a config.Config. It is pure so it can
// be unit-tested independently of the interactive prompts.
func BuildConfig(sourceConn, replicationMethod string, sels []Selection) *config.Config {
	cfg := &config.Config{}
	for _, s := range sels {
		pair := config.DatabasePair{
			Source: config.SourceConfig{
				ConnectionString:  sourceConn,
				Database:          s.SourceDB,
				ReplicationMethod: replicationMethod,
			},
			Target: config.TargetConfig{
				ConnectionString: s.TargetConn,
				Database:         s.TargetDB,
				CollectionTuning: s.CollectionTuning,
			},
		}
		if !s.AllCollections {
			for _, c := range s.Collections {
				pair.Target.Collections = append(pair.Target.Collections, config.CollectionConfig{
					SourceCollection: c,
					TargetCollection: c,
				})
			}
		}
		cfg.DatabasePairs = append(cfg.DatabasePairs, pair)
	}
	// This config is fed straight to the migrator (never round-tripped through
	// LoadConfig), so fill in tuning defaults here or zero intervals/worker
	// counts would reach incremental replication.
	config.ApplyDefaults(cfg)
	return cfg
}

// systemDBs are databases the wizard never offers for migration.
var systemDBs = map[string]bool{"admin": true, "local": true, "config": true}

// Run executes the interactive wizard, writing the generated config to
// outputPath. It reads from stdin and writes prompts to stdout.
func Run(log *logger.Logger, outputPath string) error {
	in := bufio.NewReader(os.Stdin)

	fmt.Println("=== Firestore 容灾迁移配置向导 (regional → multi-region) ===")
	fmt.Println("按提示输入；方括号内为默认值，直接回车即接受。")
	fmt.Println()

	sourceConn := prompt(in, "源 Firestore(单区域) 连接串 (mongodb://...&retryWrites=false)", "")
	if strings.TrimSpace(sourceConn) == "" {
		return fmt.Errorf("源连接串不能为空")
	}

	// Detect version/topology and pick a replication method.
	fmt.Println("正在检测源服务器版本与拓扑...")
	info, err := db.DetectSourceServer(sourceConn, "admin")
	if err != nil {
		return fmt.Errorf("无法连接/检测源服务器: %w", err)
	}
	decision := migration.ResolveReplicationMethod(info)
	fmt.Printf("  版本: %s  副本集: %v  复制方式: %s\n", info.Version, info.IsReplicaSet, decision.Method)
	if decision.Warning != "" {
		fmt.Printf("  ⚠ %s\n", decision.Warning)
	}
	fmt.Println()

	// Discover databases/collections (modern driver only; old servers fall back
	// to manual entry).
	inventory, err := introspect(sourceConn, info)
	if err != nil {
		fmt.Printf("  ⚠ 自动列举数据库失败 (%v)，将改为手动输入库/表名。\n", err)
		inventory = nil
	}

	sels, err := collectSelections(in, inventory)
	if err != nil {
		return err
	}
	if len(sels) == 0 {
		return fmt.Errorf("未选择任何数据库，已取消")
	}

	cfg := BuildConfig(sourceConn, decision.Method, sels)

	// Validate the generated config by round-tripping through the loader-shaped
	// structs (marshal here; the loader validates on real runs).
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("序列化配置失败: %w", err)
	}

	if fileExists(outputPath) {
		if !yesNo(in, fmt.Sprintf("文件 %s 已存在，覆盖?", outputPath), false) {
			return fmt.Errorf("用户取消，未覆盖 %s", outputPath)
		}
	}
	if err := os.WriteFile(outputPath, data, 0644); err != nil {
		return fmt.Errorf("写入配置失败: %w", err)
	}

	fmt.Printf("\n✅ 已生成配置: %s\n", outputPath)
	fmt.Printf("   下一步:  migrate -config=%s -mode=migrate   (全量)\n", outputPath)
	fmt.Printf("            migrate -config=%s -mode=live      (全量+增量)\n", outputPath)
	return nil
}

// introspect lists non-system databases and their collections using the modern
// driver. Returns an error for servers only reachable via the legacy driver.
func introspect(sourceConn string, info *db.SourceServerInfo) (map[string][]string, error) {
	if !info.ModernDriver {
		return nil, fmt.Errorf("源服务器仅可通过旧版驱动访问，跳过自动列举")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Use a throwaway logger-less connection via the shared MongoDB wrapper.
	m, err := db.NewMongoDB(sourceConn, "admin", 0, 4, 30*time.Second, nil, logger.New())
	if err != nil {
		return nil, err
	}
	client := m.GetClient()

	dbNames, err := client.ListDatabaseNames(ctx, bson.D{})
	if err != nil {
		return nil, err
	}
	inv := make(map[string][]string)
	for _, dbn := range dbNames {
		if systemDBs[dbn] {
			continue
		}
		colls, err := client.Database(dbn).ListCollectionNames(ctx, bson.D{})
		if err != nil {
			return nil, err
		}
		sort.Strings(colls)
		inv[dbn] = colls
	}
	return inv, nil
}

// collectSelections drives the per-database prompts.
func collectSelections(in *bufio.Reader, inventory map[string][]string) ([]Selection, error) {
	var sels []Selection

	var dbNames []string
	for name := range inventory {
		dbNames = append(dbNames, name)
	}
	sort.Strings(dbNames)

	if inventory == nil {
		// Manual mode: operator types database names one at a time.
		fmt.Println("手动模式：逐个输入要迁移的源数据库名，空行结束。")
		for {
			dbn := prompt(in, "源数据库名 (空行结束)", "")
			if strings.TrimSpace(dbn) == "" {
				break
			}
			sel, err := selectionForDB(in, dbn, nil)
			if err != nil {
				return nil, err
			}
			sels = append(sels, sel)
		}
		return sels, nil
	}

	if len(dbNames) == 0 {
		return nil, fmt.Errorf("源中没有可迁移的用户数据库")
	}

	fmt.Printf("发现 %d 个用户数据库:\n", len(dbNames))
	for _, name := range dbNames {
		fmt.Printf("  - %s (%d 个集合)\n", name, len(inventory[name]))
	}
	fmt.Println()

	for _, name := range dbNames {
		if !yesNo(in, fmt.Sprintf("迁移数据库 %q?", name), true) {
			continue
		}
		sel, err := selectionForDB(in, name, inventory[name])
		if err != nil {
			return nil, err
		}
		sels = append(sels, sel)
	}
	return sels, nil
}

// selectionForDB gathers collection choice and target details for one database.
func selectionForDB(in *bufio.Reader, sourceDB string, colls []string) (Selection, error) {
	sel := Selection{SourceDB: sourceDB}

	if len(colls) == 0 {
		// Unknown collection list (manual mode) — offer all, or comma list.
		if yesNo(in, "迁移该库的全部集合?", true) {
			sel.AllCollections = true
		} else {
			raw := prompt(in, "要迁移的集合名 (逗号分隔)", "")
			sel.Collections = splitList(raw)
			if len(sel.Collections) == 0 {
				sel.AllCollections = true
			}
		}
	} else {
		if yesNo(in, fmt.Sprintf("迁移 %q 的全部 %d 个集合?", sourceDB, len(colls)), true) {
			sel.AllCollections = true
		} else {
			for i, c := range colls {
				fmt.Printf("    [%d] %s\n", i+1, c)
			}
			raw := prompt(in, "选择集合编号 (逗号分隔, 如 1,3,4)", "")
			sel.Collections = pickByIndex(colls, raw)
			if len(sel.Collections) == 0 {
				fmt.Println("    未选择任何集合，默认迁移全部。")
				sel.AllCollections = true
			}
		}
	}

	// Target details. In this fork the target is always a (pre-created, empty)
	// multi-region Firestore database.
	if yesNo(in, "目标是多区域 Firestore (MongoDB 兼容, 需已建好空库)?", true) {
		suggested := util.SanitizeFirestoreDBID(sourceDB)
		host := prompt(in, "目标 Firestore Endpoint (uid.<multi-region-location>.firestore.goog)", "")
		dbID := prompt(in, "目标 Firestore 数据库 ID", suggested)
		// OIDC service-account auth is recommended for long-running migrations:
		// a SCRAM password can expire mid-run (DESIGN §6).
		if yesNo(in, "使用服务账号 OIDC 认证 (长跑推荐, 免密码)?", true) {
			sel.TargetConn = util.BuildFirestoreURIOIDC(host, dbID)
		} else {
			user := prompt(in, "用户名", "")
			pass := prompt(in, "密码", "")
			sel.TargetConn = util.BuildFirestoreURI(host, dbID, user, pass)
		}
		sel.TargetDB = dbID
	} else {
		sel.TargetConn = prompt(in, "目标连接串", "")
		sel.TargetDB = prompt(in, "目标数据库名", sourceDB)
	}
	return sel, nil
}

// --- small input helpers ---

func prompt(in *bufio.Reader, label, def string) string {
	if def != "" {
		fmt.Printf("%s [%s]: ", label, def)
	} else {
		fmt.Printf("%s: ", label)
	}
	line, err := in.ReadString('\n')
	if err != nil && err != io.EOF {
		return def
	}
	line = strings.TrimSpace(line)
	if line == "" {
		return def
	}
	return line
}

func yesNo(in *bufio.Reader, label string, def bool) bool {
	d := "y/N"
	if def {
		d = "Y/n"
	}
	fmt.Printf("%s [%s]: ", label, d)
	line, err := in.ReadString('\n')
	if err != nil && err != io.EOF {
		return def
	}
	line = strings.ToLower(strings.TrimSpace(line))
	switch line {
	case "":
		return def
	case "y", "yes":
		return true
	case "n", "no":
		return false
	default:
		return def
	}
}

func splitList(raw string) []string {
	var out []string
	for _, p := range strings.Split(raw, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func pickByIndex(items []string, raw string) []string {
	var out []string
	seen := map[int]bool{}
	for _, p := range strings.Split(raw, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		n, err := strconv.Atoi(p)
		if err != nil || n < 1 || n > len(items) || seen[n] {
			continue
		}
		seen[n] = true
		out = append(out, items[n-1])
	}
	return out
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
