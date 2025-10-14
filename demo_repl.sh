#!/bin/bash
# TextBank REPL Demo Script
#
# This script demonstrates all the features of the TextBank Interactive REPL
# Run this script to see an automated demo of the REPL capabilities

set -e

echo "🚀 TextBank REPL Demo"
echo "===================="
echo ""
echo "This demo will showcase the interactive REPL capabilities of TextBank."
echo "The REPL provides a user-friendly interface for storing, retrieving, and searching multilingual text."
echo ""

# Build the REPL if needed
echo "📦 Building TextBank REPL..."
cargo build --bin repl --quiet

# Check if expect is available for automation
if ! command -v expect > /dev/null; then
    echo "⚠️  'expect' not found. Install it with:"
    echo "   macOS: brew install expect"
    echo "   Ubuntu: sudo apt-get install expect"
    echo "   Running manual demo instead..."
    echo ""
    echo "🎯 Manual Demo Instructions:"
    echo "1. Run: cargo run --bin repl"
    echo "2. Try these commands in order:"
    echo ""
    echo "   help"
    echo "   intern en \"Welcome to TextBank!\""
    echo "   intern fr \"Bienvenue à TextBank!\""
    echo "   intern es \"¡Bienvenido a TextBank!\""
    echo "   get 1"
    echo "   getall 1"
    echo "   search \"welcome.*textbank\""
    echo "   search \"bienvenue\" fr"
    echo "   stats"
    echo "   quit"
    echo ""
    exit 0
fi

echo "🎬 Starting automated demo..."
echo ""

# Create the expect script
cat > /tmp/textbank_demo.exp << 'EOF'
#!/usr/bin/expect -f
set timeout 15

# Start the REPL
spawn ./target/debug/repl
expect "textbank>"

# Demo 1: Help command
puts "\n🔍 Demo 1: Getting help"
send "help\r"
expect "Examples:"
expect "textbank>"
sleep 1

# Demo 2: Adding multilingual content
puts "\n📝 Demo 2: Adding multilingual content"
send "intern en \"Welcome to TextBank - the multilingual text storage service!\"\r"
expect -re "Text .* with ID: (\[0-9\]+)"
set welcome_id $expect_out(1,string)
expect "textbank>"
sleep 1

send "intern fr \"Bienvenue à TextBank - le service de stockage de texte multilingue!\"\r"
expect -re "Text .* with ID: (\[0-9\]+)"
expect "textbank>"
sleep 1

send "intern es \"¡Bienvenido a TextBank - el servicio de almacenamiento de texto multilingüe!\"\r"
expect -re "Text .* with ID: (\[0-9\]+)"
expect "textbank>"
sleep 1

# Demo 3: Adding translations to existing ID
puts "\n🌍 Demo 3: Adding multiple languages to the same ID"
send "intern $welcome_id de \"Willkommen bei TextBank - dem mehrsprachigen Textspeicherdienst!\"\r"
expect "textbank>"
sleep 1

send "intern $welcome_id ja \"TextBankへようこそ - 多言語テキストストレージサービスです！\"\r"
expect "textbank>"
sleep 1

# Demo 4: Retrieving text by ID
puts "\n📖 Demo 4: Retrieving text by ID"
send "get $welcome_id\r"
expect "Welcome to TextBank"
expect "textbank>"
sleep 1

send "get $welcome_id fr\r"
expect "Bienvenue"
expect "textbank>"
sleep 1

send "get $welcome_id ja\r"
expect "TextBank"
expect "textbank>"
sleep 1

# Demo 5: Getting all translations
puts "\n🌐 Demo 5: Getting all translations for an ID"
send "getall $welcome_id\r"
expect "Found"
expect "translations"
expect "textbank>"
sleep 2

# Demo 6: Search functionality
puts "\n🔍 Demo 6: Searching with regular expressions"
send "search \"welcome.*textbank\"\r"
expect "match"
expect "textbank>"
sleep 1

send "search \"bienvenue.*textbank\" fr\r"
expect "match"
expect "textbank>"
sleep 1

send "search \"テキスト\" ja\r"
expect "match"
expect "textbank>"
sleep 1

# Demo 7: Case-sensitive search
puts "\n🔤 Demo 7: Case-sensitive search patterns"
send "search \"(?-i)Welcome\"\r"
expect "textbank>"
sleep 1

send "search \"welcome\"\r"
expect "textbank>"
sleep 1

# Demo 8: Advanced regex patterns
puts "\n🎯 Demo 8: Advanced regex patterns"
send "search \"^¡.*!\$\"\r"
expect "textbank>"
sleep 1

# Demo 9: Stats command
puts "\n📊 Demo 9: Service statistics"
send "stats\r"
expect "Service responding"
expect "textbank>"
sleep 1

# Demo 10: Adding some test data for final search demo
puts "\n🧪 Demo 10: Adding test data and complex searches"
send "intern en \"The quick brown fox jumps over the lazy dog\"\r"
expect "textbank>"
send "intern en \"Pack my box with five dozen liquor jugs\"\r"
expect "textbank>"
send "intern en \"How vexingly quick daft zebras jump!\"\r"
expect "textbank>"
sleep 1

send "search \"\\\\b\\\\w{4}\\\\b\"\r"
expect "textbank>"
sleep 1

send "search \"^The.*dog\$\"\r"
expect "textbank>"
sleep 1

# Exit gracefully
puts "\n👋 Demo complete - exiting REPL"
send "quit\r"
expect "Goodbye!"
expect eof

puts "\n✅ Demo completed successfully!"
EOF

chmod +x /tmp/textbank_demo.exp

echo "Running interactive demo..."
/tmp/textbank_demo.exp

# Clean up
rm -f /tmp/textbank_demo.exp

echo ""
echo "🎉 Demo Complete!"
echo "==============="
echo ""
echo "The demo showcased the following TextBank REPL features:"
echo ""
echo "✅ Interactive help system"
echo "✅ Multilingual text storage (English, French, Spanish, German, Japanese)"
echo "✅ Adding multiple translations to the same text ID"
echo "✅ Retrieving text by ID with optional language filtering"
echo "✅ Getting all translations for a text ID"
echo "✅ Case-insensitive search (default behavior)"
echo "✅ Language-specific search"
echo "✅ Case-sensitive search patterns"
echo "✅ Advanced regex patterns (anchors, word boundaries, etc.)"
echo "✅ Service statistics and health checks"
echo "✅ Complex text processing with Unicode support"
echo ""
echo "🚀 Ready to try it yourself?"
echo "   Run: cargo run --bin repl"
echo ""
echo "📚 For more information, check the README.md file"
echo "🐛 Found issues? Please report them in the issue tracker"
