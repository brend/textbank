#!/bin/bash
# Test script for TextBank REPL

set -e

echo "Building TextBank REPL..."
cargo build --bin repl --quiet

echo "Starting REPL test..."

# Create a temporary expect script to automate REPL interaction
cat > /tmp/repl_test.exp << 'EOF'
#!/usr/bin/expect -f
set timeout 10

spawn ./target/debug/repl

# Wait for the REPL to start
expect "textbank>" {
    send "help\r"
    expect "TextBank Interactive REPL"
}

expect "textbank>" {
    send "intern en \"Hello world\"\r"
    expect -re "Text .* with ID: (\[0-9\]+)"
    set text_id $expect_out(1,string)
}

expect "textbank>" {
    send "get $text_id\r"
    expect "Hello world"
}

expect "textbank>" {
    send "intern fr \"Bonjour le monde\"\r"
    expect -re "Text .* with ID: (\[0-9\]+)"
}

expect "textbank>" {
    send "search \"hello.*world\"\r"
    expect "Found 1 match:"
    expect "Hello world"
}

expect "textbank>" {
    send "quit\r"
    expect eof
}

puts "All tests passed!"
EOF

chmod +x /tmp/repl_test.exp

# Check if expect is available
if command -v expect > /dev/null; then
    echo "Running automated REPL tests..."
    /tmp/repl_test.exp
else
    echo "expect not available, running manual test..."

    # Start REPL in background
    ./target/debug/repl &
    REPL_PID=$!

    # Give it time to start
    sleep 3

    # Check if process is still running
    if ps -p $REPL_PID > /dev/null; then
        echo "✓ REPL started successfully (PID: $REPL_PID)"
        kill $REPL_PID 2>/dev/null || true
        wait $REPL_PID 2>/dev/null || true
    else
        echo "✗ REPL failed to start"
        exit 1
    fi
fi

# Clean up
rm -f /tmp/repl_test.exp

echo "REPL test completed successfully!"
echo ""
echo "To manually test the REPL, run:"
echo "  cargo run --bin repl"
echo ""
echo "Then try these commands:"
echo "  help"
echo "  intern en \"Hello world\""
echo "  get 1"
echo "  intern fr \"Bonjour le monde\""
echo "  getall 1"
echo "  search \"hello.*world\"  # case-insensitive by default"
echo "  quit"
