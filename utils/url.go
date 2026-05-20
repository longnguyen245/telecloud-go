package utils

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// IsSocialMediaURL checks if a given URL is from a known social media site
func IsSocialMediaURL(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}

	host := strings.ToLower(u.Host)
	host = strings.TrimPrefix(host, "www.")

	socialDomains := []string{
		"youtube.com", "youtu.be",
		"tiktok.com",
		"facebook.com", "fb.watch", "fb.com",
		"instagram.com", "instagr.am",
		"twitter.com", "x.com",
		"twitch.tv",
		"vimeo.com",
		"dailymotion.com",
		"soundcloud.com",
		"reddit.com",
		"threads.net",
		"bilibili.com",
		"douyin.com",
		"kuai.com",
		"kuaishou.com",
	}

	for _, domain := range socialDomains {
		if host == domain || strings.HasSuffix(host, "."+domain) {
			return true
		}
	}

	return false
}

// IsUnsafeIP reports whether an IP is loopback, link-local, multicast,
// unspecified or RFC1918/RFC4193 private. Centralized so the dial-time check
// in SafeHTTPClient and the pre-flight check in IsPrivateIP agree, and so
// callers that build their own dialer can defend against DNS rebinding too.
func IsUnsafeIP(ip net.IP) bool {
	if ip == nil {
		return true
	}
	return ip.IsLoopback() ||
		ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() ||
		ip.IsInterfaceLocalMulticast() ||
		ip.IsMulticast() ||
		ip.IsUnspecified() ||
		ip.IsPrivate()
}

// SafeDialContext returns a DialContext function that re-resolves the host at
// dial time and rejects any connection whose target IP is private/loopback.
// This closes the TOCTOU window between IsPrivateIP and http.Client.Do where a
// hostile DNS server could rotate answers ("DNS rebinding").
func SafeDialContext(timeout time.Duration) func(ctx context.Context, network, addr string) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout, KeepAlive: 30 * time.Second}
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}
		// If addr is already a literal IP, validate directly.
		if ip := net.ParseIP(host); ip != nil {
			if IsUnsafeIP(ip) {
				return nil, fmt.Errorf("ssrf: refusing to dial private IP %s", ip)
			}
			return dialer.DialContext(ctx, network, addr)
		}
		ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
		if err != nil {
			return nil, err
		}
		for _, ip := range ips {
			if IsUnsafeIP(ip.IP) {
				return nil, fmt.Errorf("ssrf: %s resolves to private IP %s", host, ip.IP)
			}
		}
		// Pin to the first resolved IP so the kernel doesn't re-resolve.
		return dialer.DialContext(ctx, network, net.JoinHostPort(ips[0].IP.String(), port))
	}
}

// SafeHTTPClient returns an http.Client whose Transport rejects connections to
// private/loopback IPs at dial time. Use it for any feature that fetches a
// URL supplied by an end user (remote upload, yt-dlp metadata probe, etc.).
//
// The redirect chain is also validated host-by-host using IsPrivateIP so a
// public host can't 302 to http://169.254.169.254/.
func SafeHTTPClient(timeout time.Duration) *http.Client {
	transport := &http.Transport{
		DialContext:           SafeDialContext(timeout),
		MaxIdleConns:          10,
		IdleConnTimeout:       30 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	return &http.Client{
		Timeout:   timeout,
		Transport: transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 10 {
				return fmt.Errorf("too many redirects")
			}
			if IsPrivateIP(req.URL.String()) {
				return fmt.Errorf("ssrf: redirect to private URL %s blocked", req.URL)
			}
			return nil
		},
	}
}

// IsPrivateIP checks if a URL points to a private/local IP address (SSRF protection).
// Note: this is best-effort — use SafeHTTPClient for the actual fetch so a
// hostile DNS server can't bypass the check via rebinding.
func IsPrivateIP(urlStr string) bool {
	u, err := url.Parse(urlStr)
	if err != nil {
		return true
	}
	hostname := u.Hostname()
	if hostname == "localhost" || hostname == "127.0.0.1" || hostname == "::1" {
		return true
	}

	// Check if the hostname is directly an IP address
	if ip := net.ParseIP(hostname); ip != nil {
		return IsUnsafeIP(ip)
	}

	ips, err := net.LookupIP(hostname)
	if err != nil {
		// On some environments, DNS lookup might fail. If we can't look it up, we allow it to proceed.
		// If it's truly an invalid domain, the HTTP client will fail to connect anyway.
		return false
	}
	for _, ip := range ips {
		if IsUnsafeIP(ip) {
			return true
		}
	}
	return false
}
