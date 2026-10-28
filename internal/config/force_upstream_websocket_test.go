package config

import (
	"testing"

	"gopkg.in/yaml.v3"
)

func TestForceUpstreamWebsocketConfigLayouts(t *testing.T) {
	cases := []struct {
		name string
		raw  string
	}{
		{
			name: "canonical upstream",
			raw:  "upstream:\n  codex:\n    force-upstream-websocket: true\n",
		},
		{
			name: "historical oauth provider",
			raw:  "oauth:\n  providers:\n    codex:\n      force-upstream-websocket: true\n",
		},
		{
			name: "legacy root",
			raw:  "codex:\n  force-upstream-websocket: true\n",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, errParse := ParseConfigBytes([]byte(tc.raw))
			if errParse != nil {
				t.Fatalf("parse config: %v", errParse)
			}
			if !cfg.Codex.ForceUpstreamWebsocket {
				t.Fatal("force-upstream-websocket did not reach the runtime config")
			}

			migrated, changed, errMigrate := NormalizeConfigLayout([]byte(tc.raw), true)
			if errMigrate != nil {
				t.Fatalf("migrate config: %v", errMigrate)
			}
			if !changed {
				t.Fatal("migration reported no change")
			}
			if errValidate := ValidateV8Config(migrated); errValidate != nil {
				t.Fatalf("validate migrated config: %v", errValidate)
			}
			var doc yaml.Node
			if errDecode := yaml.Unmarshal(migrated, &doc); errDecode != nil {
				t.Fatalf("decode migrated config: %v", errDecode)
			}
			root := doc.Content[0]
			if yamlPath(root, "upstream.codex.force-upstream-websocket") == nil {
				t.Fatalf("migrated config lost the canonical path:\n%s", migrated)
			}
			if yamlPath(root, "codex.force-upstream-websocket") != nil || yamlPath(root, "oauth.providers.codex.force-upstream-websocket") != nil {
				t.Fatalf("migrated config kept a historical path:\n%s", migrated)
			}

			restored, errRestore := ParseConfigBytes(migrated)
			if errRestore != nil {
				t.Fatalf("parse migrated config: %v", errRestore)
			}
			if !restored.Codex.ForceUpstreamWebsocket {
				t.Fatal("migrated config lost force-upstream-websocket")
			}
		})
	}
}
