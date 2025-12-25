package commitlog

import (
	"bytes"
	"testing"
)

// Helper function to create a test message with given fields
func createTestMessage(magicByte int8, attributes int8, timestamp int64, key, value []byte) Message {
	buf := new(bytes.Buffer)

	// Reserve space for CRC (will be calculated later)
	crcBytes := make([]byte, 4)
	buf.Write(crcBytes)

	// Write magic byte
	buf.WriteByte(byte(magicByte))

	// Write attributes
	buf.WriteByte(byte(attributes))

	// Write timestamp if magic byte > 0
	if magicByte > 0 {
		timestampBytes := make([]byte, 8)
		Encoding.PutUint64(timestampBytes, uint64(timestamp))
		buf.Write(timestampBytes)
	}

	// Write key length and key
	keyLenBytes := make([]byte, 4)
	if key == nil {
		Encoding.PutUint32(keyLenBytes, 0xFFFFFFFF) // -1 as uint32
	} else {
		Encoding.PutUint32(keyLenBytes, uint32(len(key)))
	}
	buf.Write(keyLenBytes)
	if key != nil {
		buf.Write(key)
	}

	// Write value length and value
	valueLenBytes := make([]byte, 4)
	if value == nil {
		Encoding.PutUint32(valueLenBytes, 0xFFFFFFFF) // -1 as uint32
	} else {
		Encoding.PutUint32(valueLenBytes, uint32(len(value)))
	}
	buf.Write(valueLenBytes)
	if value != nil {
		buf.Write(value)
	}

	// Calculate and set CRC (simplified - just use a dummy value for testing)
	message := buf.Bytes()
	crc := uint32(0x12345678) // Dummy CRC for testing
	Encoding.PutUint32(message[0:4], crc)

	return Message(message)
}

func TestNewMessage(t *testing.T) {
	data := []byte{1, 2, 3, 4, 5}
	msg := NewMessage(data)

	if !bytes.Equal([]byte(msg), data) {
		t.Errorf("NewMessage() failed: expected %v, got %v", data, []byte(msg))
	}
}

func TestMessageCrc(t *testing.T) {
	// Create a message with known CRC value
	data := make([]byte, 8)
	expectedCrc := uint32(0x12345678)
	Encoding.PutUint32(data[0:4], expectedCrc)

	msg := Message(data)
	actualCrc := msg.Crc()

	if actualCrc != int32(expectedCrc) {
		t.Errorf("Crc() failed: expected %d, got %d", expectedCrc, actualCrc)
	}
}

func TestMessageMagicByte(t *testing.T) {
	tests := []struct {
		name      string
		magicByte int8
	}{
		{"magic byte 0", 0},
		{"magic byte 1", 1},
		{"magic byte 2", 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := createTestMessage(tt.magicByte, 0, 0, nil, nil)
			actualMagic := msg.MagicByte()

			if actualMagic != tt.magicByte {
				t.Errorf("MagicByte() failed: expected %d, got %d", tt.magicByte, actualMagic)
			}
		})
	}
}

func TestMessageAttributes(t *testing.T) {
	tests := []struct {
		name       string
		attributes int8
	}{
		{"attributes 0", 0},
		{"attributes 1", 1},
		{"attributes with compression", 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := createTestMessage(1, tt.attributes, 0, nil, nil)
			actualAttributes := msg.Attributes()

			if actualAttributes != tt.attributes {
				t.Errorf("Attributes() failed: expected %d, got %d", tt.attributes, actualAttributes)
			}
		})
	}
}

func TestMessageTimestamp(t *testing.T) {
	tests := []struct {
		name        string
		magicByte   int8
		timestamp   int64
		shouldPanic bool
	}{
		{"v0 message should panic", 0, 123456789, true},
		{"v1 message with timestamp", 1, 123456789, false},
		{"v2 message with timestamp", 2, 987654321, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := createTestMessage(tt.magicByte, 0, tt.timestamp, nil, nil)

			if tt.shouldPanic {
				defer func() {
					if r := recover(); r == nil {
						t.Errorf("Timestamp() should have panicked for v0 message")
					}
				}()
				msg.Timestamp()
			} else {
				actualTimestamp := msg.Timestamp()
				if actualTimestamp != tt.timestamp {
					t.Errorf("Timestamp() failed: expected %d, got %d", tt.timestamp, actualTimestamp)
				}
			}
		})
	}
}

func TestMessageKey(t *testing.T) {
	tests := []struct {
		name string
		key  []byte
	}{
		{"nil key", nil},
		{"empty key", []byte{}},
		{"simple key", []byte("test-key")},
		{"binary key", []byte{0x01, 0x02, 0x03, 0xFF}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := createTestMessage(1, 0, 0, tt.key, []byte("value"))
			actualKey := msg.Key()

			if tt.key == nil {
				if actualKey != nil {
					t.Errorf("Key() failed: expected nil, got %v", actualKey)
				}
			} else if !bytes.Equal(actualKey, tt.key) {
				t.Errorf("Key() failed: expected %v, got %v", tt.key, actualKey)
			}
		})
	}
}

func TestMessageValue(t *testing.T) {
	tests := []struct {
		name  string
		value []byte
	}{
		{"nil value", nil},
		{"empty value", []byte{}},
		{"simple value", []byte("test-value")},
		{"json value", []byte(`{"name": "test", "id": 123}`)},
		{"binary value", []byte{0xFF, 0xFE, 0xFD, 0x00, 0x01}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := createTestMessage(1, 0, 0, []byte("key"), tt.value)
			actualValue := msg.Value()

			if tt.value == nil {
				if actualValue != nil {
					t.Errorf("Value() failed: expected nil, got %v", actualValue)
				}
			} else if !bytes.Equal(actualValue, tt.value) {
				t.Errorf("Value() failed: expected %v, got %v", tt.value, actualValue)
			}
		})
	}
}

func TestMessageSize(t *testing.T) {
	tests := []struct {
		name      string
		magicByte int8
		key       []byte
		value     []byte
		expected  int32
	}{
		{
			name:      "v0 message with nil key and value",
			magicByte: 0,
			key:       nil,
			value:     nil,
			expected:  4 + 1 + 1 + 4 + 4, // crc + magic + attributes + key_len + value_len
		},
		{
			name:      "v1 message with nil key and value",
			magicByte: 1,
			key:       nil,
			value:     nil,
			expected:  4 + 1 + 1 + 8 + 4 + 4, // crc + magic + attributes + timestamp + key_len + value_len
		},
		{
			name:      "v1 message with key and value",
			magicByte: 1,
			key:       []byte("test"),
			value:     []byte("message"),
			expected:  4 + 1 + 1 + 8 + 4 + 4 + 4 + 7, // base + key_len + key + value_len + value
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := createTestMessage(tt.magicByte, 0, 0, tt.key, tt.value)
			actualSize := msg.Size()

			if actualSize != tt.expected {
				t.Errorf("Size() failed: expected %d, got %d", tt.expected, actualSize)
			}
		})
	}
}

func TestMessageKeyOffsets(t *testing.T) {
	tests := []struct {
		name      string
		magicByte int8
		key       []byte
	}{
		{"v0 message with key", 0, []byte("test")},
		{"v1 message with key", 1, []byte("test")},
		{"v0 message with nil key", 0, nil},
		{"v1 message with nil key", 1, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := createTestMessage(tt.magicByte, 0, 0, tt.key, []byte("value"))
			start, end, size := msg.keyOffsets()

			// Verify start position based on magic byte
			expectedStart := int32(6)
			if tt.magicByte > 0 {
				expectedStart = 14
			}

			if start != expectedStart {
				t.Errorf("keyOffsets() start failed: expected %d, got %d", expectedStart, start)
			}

			// Verify size
			expectedSize := int32(-1)
			if tt.key != nil {
				expectedSize = int32(len(tt.key))
			}

			if size != expectedSize {
				t.Errorf("keyOffsets() size failed: expected %d, got %d", expectedSize, size)
			}

			// Verify end position
			expectedEnd := start + 4 + size
			if end != expectedEnd {
				t.Errorf("keyOffsets() end failed: expected %d, got %d", expectedEnd, end)
			}
		})
	}
}

func TestMessageValueOffsets(t *testing.T) {
	tests := []struct {
		name  string
		key   []byte
		value []byte
	}{
		{"with both key and value", []byte("key"), []byte("value")},
		{"with key and nil value", []byte("key"), nil},
		{"with empty key and value", []byte(""), []byte("value")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := createTestMessage(1, 0, 0, tt.key, tt.value)
			start, end, size := msg.valueOffsets()

			// Get key offsets to verify value start position
			_, keyEnd, _ := msg.keyOffsets()

			if start != keyEnd {
				t.Errorf("valueOffsets() start failed: expected %d, got %d", keyEnd, start)
			}

			// Verify size
			expectedSize := int32(-1)
			if tt.value != nil {
				expectedSize = int32(len(tt.value))
			}

			if size != expectedSize {
				t.Errorf("valueOffsets() size failed: expected %d, got %d", expectedSize, size)
			}

			// Verify end position
			expectedEnd := start + 4 + size
			if end != expectedEnd {
				t.Errorf("valueOffsets() end failed: expected %d, got %d", expectedEnd, end)
			}
		})
	}
}

func TestMessageComplexScenario(t *testing.T) {
	// Test a complex message with all fields
	key := []byte("user:123")
	value := []byte(`{"name": "John Doe", "age": 30, "email": "john@example.com"}`)
	timestamp := int64(1609459200000) // 2021-01-01 00:00:00 UTC
	attributes := int8(2)             // Some compression flag
	magicByte := int8(1)

	msg := createTestMessage(magicByte, attributes, timestamp, key, value)

	// Verify all fields
	if msg.MagicByte() != magicByte {
		t.Errorf("Complex message MagicByte failed: expected %d, got %d", magicByte, msg.MagicByte())
	}

	if msg.Attributes() != attributes {
		t.Errorf("Complex message Attributes failed: expected %d, got %d", attributes, msg.Attributes())
	}

	if msg.Timestamp() != timestamp {
		t.Errorf("Complex message Timestamp failed: expected %d, got %d", timestamp, msg.Timestamp())
	}

	if !bytes.Equal(msg.Key(), key) {
		t.Errorf("Complex message Key failed: expected %v, got %v", key, msg.Key())
	}

	if !bytes.Equal(msg.Value(), value) {
		t.Errorf("Complex message Value failed: expected %v, got %v", value, msg.Value())
	}

	// Verify size calculation
	expectedSize := int32(4 + 1 + 1 + 8 + 4 + len(key) + 4 + len(value))
	if msg.Size() != expectedSize {
		t.Errorf("Complex message Size failed: expected %d, got %d", expectedSize, msg.Size())
	}
}

func TestMessageEdgeCases(t *testing.T) {
	t.Run("very long key and value", func(t *testing.T) {
		key := make([]byte, 1024)
		value := make([]byte, 4096)
		for i := range key {
			key[i] = byte(i % 256)
		}
		for i := range value {
			value[i] = byte((i + 128) % 256)
		}

		msg := createTestMessage(1, 0, 0, key, value)

		if !bytes.Equal(msg.Key(), key) {
			t.Error("Long key not preserved correctly")
		}

		if !bytes.Equal(msg.Value(), value) {
			t.Error("Long value not preserved correctly")
		}
	})

	t.Run("empty key with value", func(t *testing.T) {
		key := []byte{}
		value := []byte("test")

		msg := createTestMessage(1, 0, 0, key, value)

		if !bytes.Equal(msg.Key(), key) {
			t.Errorf("Empty key failed: expected %v, got %v", key, msg.Key())
		}

		if !bytes.Equal(msg.Value(), value) {
			t.Errorf("Value with empty key failed: expected %v, got %v", value, msg.Value())
		}
	})
}
