package ws

import "testing"

func TestNormalizeChannels(t *testing.T) {
	t.Parallel()

	channels := normalizeChannels([]string{" Alerts ", "dashboard", "alerts", ""})
	if len(channels) != 2 {
		t.Fatalf("unexpected channel count: %d", len(channels))
	}
	if channels[0] != "alerts" || channels[1] != "dashboard" {
		t.Fatalf("unexpected channels: %#v", channels)
	}
}

func TestNormalizeChannelsDefaultsToDashboard(t *testing.T) {
	t.Parallel()

	channels := normalizeChannels(nil)
	if len(channels) != 1 || channels[0] != "dashboard" {
		t.Fatalf("unexpected default channels: %#v", channels)
	}
}

func TestClientMatchesUserAndChannels(t *testing.T) {
	t.Parallel()

	client := &client{
		info: ClientInfo{
			ConnectionID: "ws-1",
			UserID:       "user-demo",
		},
		channels: map[string]struct{}{
			"alerts":    {},
			"dashboard": {},
		},
	}

	if !client.matches("user-demo", []string{"alerts"}) {
		t.Fatal("expected user/channel match")
	}
	if client.matches("another-user", []string{"alerts"}) {
		t.Fatal("did not expect different user to match")
	}
	if client.matches("user-demo", []string{"transactions"}) {
		t.Fatal("did not expect unmatched channel to match")
	}
}
