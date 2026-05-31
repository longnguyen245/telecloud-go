package tgclient

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"telecloud/config"
	"telecloud/database"

	"github.com/gotd/td/tg"
)

var (
	locationCache = make(map[int]*cachedLocation)
	cacheMutex    sync.RWMutex
)

func init() {
	// Dọn dẹp location cache expired mỗi 30 phút
	go func() {
		ticker := time.NewTicker(30 * time.Minute)
		for range ticker.C {
			now := time.Now()
			cacheMutex.Lock()
			for k, v := range locationCache {
				if now.After(v.expiresAt) {
					delete(locationCache, k)
				}
			}
			cacheMutex.Unlock()
		}
	}()
}

type cachedLocation struct {
	loc       tg.InputFileLocationClass
	api       *tg.Client // Store the API client that resolved this location
	expiresAt time.Time
}

type tgFileReader struct {
	ctx    context.Context
	cancel context.CancelFunc
	api    *tg.Client
	loc    tg.InputFileLocationClass
	size   int64
	offset int64

	// Current chunk (served synchronously)
	chunkData   []byte
	chunkOffset int64

	// Prefetch buffer
	prefetchChunks map[int64][]byte // offset → prefetched chunk data
	prefetchMu     sync.Mutex
	prefetchSem    chan struct{} // capacity 1: ensures only one prefetch goroutine at a time
}

func (r *tgFileReader) Close() error {
	if r.cancel != nil {
		r.cancel()
	}
	return nil
}

func (r *tgFileReader) Read(p []byte) (int, error) {
	if r.offset >= r.size {
		return 0, io.EOF
	}

	const chunkSize = int64(1024 * 1024)

	// If we have no data or the current offset is outside our cached chunk, load it
	if r.chunkData == nil || r.offset < r.chunkOffset || r.offset >= r.chunkOffset+int64(len(r.chunkData)) {
		chunkStart := (r.offset / chunkSize) * chunkSize

		// Try to get the chunk from the prefetch buffer
		r.prefetchMu.Lock()
		if data, ok := r.prefetchChunks[chunkStart]; ok {
			if r.offset >= chunkStart+int64(len(data)) {
				delete(r.prefetchChunks, chunkStart)
				r.prefetchMu.Unlock()
				return 0, io.ErrUnexpectedEOF
			}
			r.chunkData = data
			r.chunkOffset = chunkStart
			delete(r.prefetchChunks, chunkStart)
			r.prefetchMu.Unlock()
		} else {
			r.prefetchMu.Unlock()
			// Fallback: synchronous fetch with retries
			data, err := r.fetchChunk(r.api, chunkStart, chunkSize)
			if err != nil {
				return 0, err
			}
			if r.offset >= chunkStart+int64(len(data)) {
				return 0, io.ErrUnexpectedEOF
			}
			r.chunkData = data
			r.chunkOffset = chunkStart
		}
	}

	// Copy data to caller's buffer
	inChunkOffset := r.offset - r.chunkOffset
	n := copy(p, r.chunkData[inChunkOffset:])
	r.offset += int64(n)

	// Trigger prefetch for the NEXT chunk when we reach the midpoint of current chunk.
	// Uses a different bot than the sync fallback to spread rate-limit across sessions.
	if inChunkOffset >= int64(len(r.chunkData))/2 {
		r.triggerPrefetch(r.chunkOffset+chunkSize, chunkSize)
	}

	return n, nil
}

// triggerPrefetch launches a single background goroutine to fetch the next chunk
// using a different bot than the synchronous fallback path. This spreads the
// Telegram rate-limit across sessions without overwhelming a single DC.
func (r *tgFileReader) triggerPrefetch(offset int64, limit int64) {
	if offset >= r.size {
		return
	}

	r.prefetchMu.Lock()
	// Already prefetched or being prefetched
	if _, exists := r.prefetchChunks[offset]; exists {
		r.prefetchMu.Unlock()
		return
	}
	r.prefetchMu.Unlock()

	// Non-blocking: if a prefetch is already running, skip
	select {
	case r.prefetchSem <- struct{}{}:
	default:
		return
	}

	go func() {
		defer func() { <-r.prefetchSem }()

		// Use a different bot than the one used for sync fallback
		fetchAPI := GetAPI()
		data, err := r.fetchChunk(fetchAPI, offset, limit)
		if err != nil {
			return
		}

		r.prefetchMu.Lock()
		r.prefetchChunks[offset] = data
		r.prefetchMu.Unlock()
	}()
}

func (r *tgFileReader) fetchChunk(api *tg.Client, offset int64, limit int64) ([]byte, error) {
	req := &tg.UploadGetFileRequest{
		Precise:  true,
		Location: r.loc,
		Offset:   offset,
		Limit:    int(limit),
	}

	var res tg.UploadFileClass
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		res, err = api.UploadGetFile(r.ctx, req)
		if err == nil {
			// Check for incomplete reads (less bytes than requested limit and not EOF)
			if fileObj, ok := res.(*tg.UploadFile); ok {
				if len(fileObj.Bytes) < int(limit) && offset+int64(len(fileObj.Bytes)) < r.size {
					err = fmt.Errorf("incomplete read from telegram: got %d bytes, expected %d", len(fileObj.Bytes), limit)
					// Sleep briefly and retry
					select {
					case <-time.After(time.Duration(attempt+1) * 500 * time.Millisecond):
						continue
					case <-r.ctx.Done():
						return nil, r.ctx.Err()
					}
				}
			}
			break
		}
		if r.ctx.Err() != nil {
			return nil, r.ctx.Err()
		}
		errStr := err.Error()
		if strings.Contains(errStr, "FLOOD_WAIT") || strings.Contains(errStr, "TIMEOUT") || strings.Contains(errStr, "RPC_CALL_FAIL") {
			waitDuration := time.Duration(attempt+1) * 2 * time.Second
			if strings.Contains(errStr, "FLOOD_WAIT_") {
				parts := strings.Split(errStr, "FLOOD_WAIT_")
				if len(parts) > 1 {
					if secs, e := fmt.Sscanf(parts[1], "%d", new(int)); e == nil && secs > 0 {
						waitDuration = time.Duration(secs) * time.Second
					}
				}
			}
			select {
			case <-time.After(waitDuration):
				continue
			case <-r.ctx.Done():
				return nil, r.ctx.Err()
			}
		}
		select {
		case <-time.After(time.Duration(attempt+1) * time.Second):
		case <-r.ctx.Done():
			return nil, r.ctx.Err()
		}
	}
	if err != nil {
		return nil, err
	}

	switch result := res.(type) {
	case *tg.UploadFile:
		if len(result.Bytes) == 0 && offset < r.size {
			return nil, fmt.Errorf("unexpected end of file from telegram at offset %d (expected %d)", offset, r.size)
		}
		return result.Bytes, nil
	case *tg.UploadFileCDNRedirect:
		return nil, fmt.Errorf("CDN redirect not supported")
	default:
		return nil, fmt.Errorf("unexpected type %T", res)
	}
}

func (r *tgFileReader) Seek(offset int64, whence int) (int64, error) {
	var newOffset int64
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = r.offset + offset
	case io.SeekEnd:
		newOffset = r.size + offset
	}
	if newOffset < 0 {
		newOffset = 0
	}
	if newOffset > r.size {
		newOffset = r.size
	}
	if newOffset != r.offset {
		r.offset = newOffset
		// Invalidate prefetch buffer and current chunk on seek
		r.prefetchMu.Lock()
		r.prefetchChunks = make(map[int64][]byte)
		r.prefetchMu.Unlock()
		r.chunkData = nil
		r.chunkOffset = 0
	}
	return r.offset, nil
}

func ServeTelegramFile(c *http.Request, w http.ResponseWriter, file database.File, cfg *config.Config) error {
	ctx := c.Context()

	reader, err := GetTelegramFileReader(ctx, file, cfg)
	if err != nil {
		return err
	}

	// Allow browser/player to seek and cache the stream
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Cache-Control", "private, max-age=3600")
	w.Header().Set("X-Accel-Buffering", "no")

	// Set Content-Type if not already set
	if w.Header().Get("Content-Type") == "" && file.MimeType != nil {
		mime := *file.MimeType
		// Fallback for common types if stored as octet-stream
		lowerName := strings.ToLower(file.Filename)
		if mime == "application/octet-stream" {
			if strings.HasSuffix(lowerName, ".pdf") {
				mime = "application/pdf"
			} else if strings.HasSuffix(lowerName, ".epub") {
				mime = "application/epub+zip"
			}
		}
		// Special handling for MKV to ensure browser compatibility (especially Safari)
		if strings.HasSuffix(lowerName, ".mkv") {
			mime = "video/mp4"
		}
		w.Header().Set("Content-Type", mime)
	}

	// Set Content-Disposition only if not already set (e.g., by router for attachment)
	if w.Header().Get("Content-Disposition") == "" {
		// Use proper RFC 6266 encoding for filename to support non-ASCII characters and quotes
		// filename*=UTF-8''... is the standard for modern browsers
		encodedName := url.PathEscape(file.Filename)
		// We still provide the quoted filename for legacy browsers
		safeName := strings.ReplaceAll(file.Filename, `"`, `\"`)
		w.Header().Set("Content-Disposition", fmt.Sprintf(`inline; filename="%s"; filename*=UTF-8''%s`, safeName, encodedName))
	}

	defer reader.Close()
	http.ServeContent(w, c, file.Filename, file.CreatedAt, reader)
	return nil
}

func GetTelegramFileReader(ctx context.Context, file database.File, cfg *config.Config) (io.ReadSeekCloser, error) {
	// Check if this file has multiple parts
	parts, err := database.GetFileParts(file.ID)
	if err == nil && len(parts) > 1 {
		ctx, cancel := context.WithCancel(ctx)
		return &multiPartReader{
			ctx:    ctx,
			cancel: cancel,
			parts:  parts,
			size:   file.Size,
			cfg:    cfg,
		}, nil
	}

	// Single part (or legacy file)
	if file.MessageID == nil {
		return nil, fmt.Errorf("file has no message ID")
	}
	return getSinglePartReader(ctx, *file.MessageID, file.Size, cfg)
}

var getSinglePartReader = func(ctx context.Context, msgID int, size int64, cfg *config.Config) (io.ReadSeekCloser, error) {
	ctx, cancel := context.WithCancel(ctx)

	// Check cache first
	cacheMutex.RLock()
	cached, ok := locationCache[msgID]
	cacheMutex.RUnlock()

	if ok && time.Now().Before(cached.expiresAt) {
		return &tgFileReader{
			ctx:            ctx,
			cancel:         cancel,
			api:            cached.api,
			loc:            cached.loc,
			size:           size,
			prefetchChunks: make(map[int64][]byte),
			prefetchSem:    make(chan struct{}, 1),
		}, nil
	}

	// Helper function to resolve media from a specific API client
	resolve := func(targetApi *tg.Client) (tg.InputFileLocationClass, error) {
		peer, err := resolveLogGroup(ctx, targetApi, cfg.LogGroupID)
		if err != nil {
			return nil, err
		}

		var msgs tg.MessageClassArray
		if channel, ok := peer.(*tg.InputPeerChannel); ok {
			res, err := targetApi.ChannelsGetMessages(ctx, &tg.ChannelsGetMessagesRequest{
				Channel: &tg.InputChannel{
					ChannelID:  channel.ChannelID,
					AccessHash: channel.AccessHash,
				},
				ID: []tg.InputMessageClass{&tg.InputMessageID{ID: msgID}},
			})
			if err != nil {
				return nil, err
			}
			switch m := res.(type) {
			case *tg.MessagesMessages:
				msgs = m.Messages
			case *tg.MessagesMessagesSlice:
				msgs = m.Messages
			case *tg.MessagesChannelMessages:
				msgs = m.Messages
			}
		} else {
			res, err := targetApi.MessagesGetMessages(ctx, []tg.InputMessageClass{&tg.InputMessageID{ID: msgID}})
			if err != nil {
				return nil, err
			}
			switch m := res.(type) {
			case *tg.MessagesMessages:
				msgs = m.Messages
			case *tg.MessagesMessagesSlice:
				msgs = m.Messages
			case *tg.MessagesChannelMessages:
				msgs = m.Messages
			}
		}

		if len(msgs) == 0 {
			return nil, fmt.Errorf("message not found")
		}

		msg, ok := msgs[0].(*tg.Message)
		if !ok || msg.Media == nil {
			// This often happens if the bot is not an admin in a group and privacy mode is on,
			// or if the message ID is invalid for this session.
			return nil, fmt.Errorf("message has no media")
		}

		if docMedia, ok := msg.Media.(*tg.MessageMediaDocument); ok {
			doc, ok := docMedia.Document.(*tg.Document)
			if !ok {
				return nil, fmt.Errorf("document is empty")
			}
			return doc.AsInputDocumentFileLocation(), nil
		}

		if photoMedia, ok := msg.Media.(*tg.MessageMediaPhoto); ok {
			photo, ok := photoMedia.Photo.(*tg.Photo)
			if !ok {
				return nil, fmt.Errorf("photo is empty")
			}
			// Find the best photo size available (largest w/h/size)
			var bestSizeClass tg.PhotoSizeClass
			var maxArea int
			for _, sz := range photo.Sizes {
				switch s := sz.(type) {
				case *tg.PhotoSize:
					area := s.W * s.H
					if area > maxArea {
						maxArea = area
						bestSizeClass = sz
					}
				case *tg.PhotoSizeProgressive:
					area := s.W * s.H
					if area > maxArea {
						maxArea = area
						bestSizeClass = sz
					}
				case *tg.PhotoCachedSize:
					area := s.W * s.H
					if area > maxArea {
						maxArea = area
						bestSizeClass = sz
					}
				}
			}

			if bestSizeClass == nil && len(photo.Sizes) > 0 {
				bestSizeClass = photo.Sizes[len(photo.Sizes)-1]
			}

			if bestSizeClass == nil {
				return nil, fmt.Errorf("no valid photo sizes found")
			}

			return &tg.InputPhotoFileLocation{
				ID:            photo.ID,
				AccessHash:    photo.AccessHash,
				FileReference: photo.FileReference,
				ThumbSize:     bestSizeClass.GetType(),
			}, nil
		}

		return nil, fmt.Errorf("media type not supported for download: %T", msg.Media)
	}

	api := GetAPI()
	loc, err := resolve(api)

	// Fallback to main client if the selected bot failed to find the message/media
	if err != nil && api != Client.API() {
		// Only retry for specific "not found" or "no media" errors which usually indicate permission issues in bot pool
		errStr := err.Error()
		if strings.Contains(errStr, "not found") || strings.Contains(errStr, "no media") {
			mainApi := Client.API()
			if locRetry, errRetry := resolve(mainApi); errRetry == nil {
				api = mainApi
				loc = locRetry
				err = nil
			}
		}
	}

	if err != nil {
		cancel()
		return nil, err
	}

	// Cache the location AND the API client for 1 hour
	cacheMutex.Lock()
	locationCache[msgID] = &cachedLocation{
		loc:       loc,
		api:       api,
		expiresAt: time.Now().Add(1 * time.Hour),
	}
	cacheMutex.Unlock()

	reader := &tgFileReader{
		ctx:            ctx,
		cancel:         cancel,
		api:            api,
		loc:            loc,
		size:           size,
		prefetchChunks: make(map[int64][]byte),
		prefetchSem:    make(chan struct{}, 1),
	}

	return reader, nil
}

type multiPartReader struct {
	ctx    context.Context
	cancel context.CancelFunc
	parts  []database.FilePart
	size   int64
	offset int64
	cfg    *config.Config

	currentReader       io.ReadSeekCloser
	currentIndex        int
	currentPartRemaining int64 // bytes left in current part before EOF

	// Pre-initialized next part reader (avoids gap between parts)
	mu              sync.Mutex
	nextReader      io.ReadSeekCloser
	nextPartIndex   int
	nextInitialized bool
}

func (r *multiPartReader) Close() error {
	if r.currentReader != nil {
		r.currentReader.Close()
	}
	
	r.mu.Lock()
	if r.nextReader != nil {
		r.nextReader.Close()
		r.nextReader = nil
	}
	r.nextPartIndex = -1
	r.nextInitialized = true // Prevent spawning any new prefetching
	r.mu.Unlock()

	if r.cancel != nil {
		r.cancel()
	}
	return nil
}

func (r *multiPartReader) Read(p []byte) (int, error) {
	if r.offset >= r.size {
		return 0, io.EOF
	}

	for {
		if r.currentReader == nil {
			// Use pre-initialized nextReader if available and matches
			r.mu.Lock()
			nextReader := r.nextReader
			nextPartIdx := r.nextPartIndex
			r.nextReader = nil
			r.nextInitialized = false
			r.mu.Unlock()

			if nextReader != nil && nextPartIdx == r.currentIndex {
				r.currentReader = nextReader
				r.currentPartRemaining = r.parts[r.currentIndex].Size
			} else {
				if nextReader != nil {
					nextReader.Close()
				}

				// Find which part contains the current offset
				var partStart int64
				found := false
				for i, part := range r.parts {
					if r.offset < partStart+part.Size {
						r.currentIndex = i
						reader, err := getSinglePartReader(r.ctx, part.MessageID, part.Size, r.cfg)
						if err != nil {
							return 0, err
						}
						relOffset := r.offset - partStart
						if relOffset > 0 {
							if _, err = reader.Seek(relOffset, io.SeekStart); err != nil {
								return 0, err
							}
						}
						r.currentReader = reader
						r.currentPartRemaining = part.Size - relOffset
						found = true
						break
					}
					partStart += part.Size
				}
				if !found {
					return 0, io.EOF
				}
			}
		}

		n, err := r.currentReader.Read(p)
		if n > 0 {
			r.offset += int64(n)
			r.currentPartRemaining -= int64(n)

			// Pre-initialize next part reader when approaching end of current part.
			// This eliminates the gap between parts by resolving the next message
			// location and prefetching its first chunk while we still have data to serve.
			const prefetchThreshold = int64(2 * 1024 * 1024) // 2MB
			nextIdx := r.currentIndex + 1
			
			r.mu.Lock()
			shouldPrefetch := !r.nextInitialized && r.currentPartRemaining <= prefetchThreshold && nextIdx < len(r.parts)
			if shouldPrefetch {
				r.nextInitialized = true
				r.nextPartIndex = nextIdx
				nextPart := r.parts[nextIdx]
				r.mu.Unlock()

				go func() {
					reader, err := getSinglePartReader(r.ctx, nextPart.MessageID, nextPart.Size, r.cfg)
					
					r.mu.Lock()
					defer r.mu.Unlock()

					// Check if reader has been closed/cancelled in the meantime
					if r.ctx.Err() != nil {
						if err == nil && reader != nil {
							reader.Close()
						}
						return
					}

					// Check if the user has seeked away to a different part index
					if r.nextPartIndex != nextIdx {
						if err == nil && reader != nil {
							reader.Close()
						}
						return
					}

					if err == nil {
						r.nextReader = reader
					} else {
						// Allow retry on error
						r.nextInitialized = false
					}
				}()
			} else {
				r.mu.Unlock()
			}

			return n, nil
		}
		if err == io.EOF {
			r.currentReader.Close()
			r.currentReader = nil
			r.currentIndex++
			r.currentPartRemaining = 0
			if r.currentIndex >= len(r.parts) {
				return 0, io.EOF
			}
			continue
		}
		return n, err
	}
}

func (r *multiPartReader) Seek(offset int64, whence int) (int64, error) {
	var newOffset int64
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = r.offset + offset
	case io.SeekEnd:
		newOffset = r.size + offset
	}

	if newOffset < 0 {
		newOffset = 0
	}
	if newOffset > r.size {
		newOffset = r.size
	}

	if newOffset != r.offset {
		r.offset = newOffset
		if r.currentReader != nil {
			r.currentReader.Close()
			r.currentReader = nil
		}
		
		r.mu.Lock()
		if r.nextReader != nil {
			r.nextReader.Close()
			r.nextReader = nil
		}
		// Reset state so prefetching can trigger for the new part
		r.nextPartIndex = -1
		r.nextInitialized = false
		r.mu.Unlock()
	}
	return r.offset, nil
}
