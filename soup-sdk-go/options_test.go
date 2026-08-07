package soup

import "testing"

// TestOptionsDefaults:未设置的项走默认值;SnapshotHz=0 保持禁用不被兜底。
func TestOptionsDefaults(t *testing.T) {
	srv := NewServer(WithGatekeeper(echoGK{}))
	if srv.cfg.TickHz != 20 {
		t.Errorf("TickHz 默认 = %d, want 20", srv.cfg.TickHz)
	}
	if srv.cfg.SnapshotHz != 10 {
		t.Errorf("SnapshotHz 默认 = %d, want 10", srv.cfg.SnapshotHz)
	}
	if srv.cfg.JitterBufferTicks != 2 {
		t.Errorf("JitterBufferTicks 默认 = %d, want 2", srv.cfg.JitterBufferTicks)
	}
	if srv.cfg.MaxRooms != 1024 {
		t.Errorf("MaxRooms 默认 = %d, want 1024", srv.cfg.MaxRooms)
	}
}

// TestOptionsOverride:显式 Option 覆盖默认;后写的覆盖先写的。
func TestOptionsOverride(t *testing.T) {
	srv := NewServer(WithTickHz(60), WithSnapshotHz(0), WithMaxRooms(8))
	if srv.cfg.TickHz != 60 {
		t.Errorf("TickHz = %d, want 60", srv.cfg.TickHz)
	}
	if srv.cfg.SnapshotHz != 0 {
		t.Errorf("SnapshotHz = %d, want 0(禁用)", srv.cfg.SnapshotHz)
	}
	if srv.cfg.MaxRooms != 8 {
		t.Errorf("MaxRooms = %d, want 8", srv.cfg.MaxRooms)
	}
}

// TestOptionsWithConfig:整体 Config 覆盖可与其它 Option 混用(后者胜)。
func TestOptionsWithConfig(t *testing.T) {
	base := Config{EngineSocket: "/tmp/x.sock", TickHz: 30, MaxRooms: 16}
	srv := NewServer(WithConfig(base), WithTickHz(45))
	if srv.cfg.EngineSocket != "/tmp/x.sock" {
		t.Errorf("EngineSocket = %q", srv.cfg.EngineSocket)
	}
	if srv.cfg.TickHz != 45 {
		t.Errorf("TickHz = %d, want 45(后者胜)", srv.cfg.TickHz)
	}
	if srv.cfg.MaxRooms != 16 {
		t.Errorf("MaxRooms = %d, want 16", srv.cfg.MaxRooms)
	}
}
