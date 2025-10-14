//! Interactive REPL for the TextBank service.
//!
//! This binary starts the TextBank gRPC service in the background and provides
//! an interactive command-line interface for adding and retrieving texts.

use anyhow::{Context, Result};
use rustyline::DefaultEditor;
use std::sync::Arc;
use std::time::Duration;
use textbank::pb::{
    text_bank_client::TextBankClient, GetAllRequest, GetRequest, InternRequest, SearchRequest,
};
use tokio::time::sleep;
use tonic::transport::Channel;

/// REPL commands and their descriptions
const HELP_TEXT: &str = r#"
TextBank Interactive REPL

Commands:
  help                           - Show this help message
  quit, exit                     - Exit the REPL
  intern <lang> <text>           - Add text in specified language
  intern <id> <lang> <text>      - Add/update text with specific ID
  get <id> [lang]                - Get text by ID, optionally in specific language
  getall <id>                    - Get all translations for an ID
  search <pattern> [lang]        - Search texts using regex pattern (case-insensitive by default)
  stats                          - Show service statistics

Examples:
  intern en "Hello world"
  intern 42 fr "Bonjour le monde"
  get 1
  get 1 fr
  getall 1
  search "hello.*world"
  search "bonjour" fr
  search "(?-i)Hello" (case-sensitive)
"#;

#[tokio::main]
async fn main() -> Result<()> {
    println!("Starting TextBank REPL...");

    // Start the TextBank service in a background task
    tokio::spawn(async {
        if let Err(e) = start_textbank_service().await {
            eprintln!("TextBank service error: {}", e);
        }
    });

    // Give the service time to start
    sleep(Duration::from_millis(500)).await;

    // Connect to the service
    let client = connect_to_service()
        .await
        .context("Failed to connect to TextBank service")?;

    println!("Connected to TextBank service!");
    println!("Type 'help' for available commands or 'quit' to exit.");

    run_repl(client).await
}

async fn start_textbank_service() -> Result<()> {
    // Import the main textbank service code
    use textbank::{pb::text_bank_server::TextBankServer, Store, Svc};
    use tonic::transport::Server;

    let store = Arc::new(Store::new("data/wal.jsonl", true)?);
    let svc = Svc { store };

    let addr = "127.0.0.1:50051".parse()?;

    println!("TextBank service listening on {}", addr);

    Server::builder()
        .add_service(TextBankServer::new(svc))
        .serve(addr)
        .await
        .context("Failed to start gRPC server")
}

async fn connect_to_service() -> Result<TextBankClient<Channel>> {
    let mut attempts = 0;
    loop {
        match TextBankClient::connect("http://127.0.0.1:50051").await {
            Ok(client) => return Ok(client),
            Err(_) if attempts < 10 => {
                attempts += 1;
                sleep(Duration::from_millis(100)).await;
            }
            Err(e) => return Err(e.into()),
        }
    }
}

async fn run_repl(mut client: TextBankClient<Channel>) -> Result<()> {
    let mut rl = DefaultEditor::new().context("Failed to create readline editor")?;

    loop {
        let readline = rl.readline("textbank> ");
        match readline {
            Ok(line) => {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }

                rl.add_history_entry(line)
                    .context("Failed to add history entry")?;

                if let Err(e) = handle_command(&mut client, line).await {
                    eprintln!("Error: {}", e);
                }
            }
            Err(rustyline::error::ReadlineError::Interrupted) => {
                println!("^C");
                continue;
            }
            Err(rustyline::error::ReadlineError::Eof) => {
                println!("Goodbye!");
                break;
            }
            Err(err) => {
                eprintln!("Error: {}", err);
                break;
            }
        }
    }

    Ok(())
}

async fn handle_command(client: &mut TextBankClient<Channel>, line: &str) -> Result<()> {
    let parts: Vec<&str> = line.split_whitespace().collect();
    if parts.is_empty() {
        return Ok(());
    }

    match parts[0] {
        "help" => {
            println!("{}", HELP_TEXT);
        }
        "quit" | "exit" => {
            println!("Goodbye!");
            std::process::exit(0);
        }
        "intern" => {
            handle_intern_command(client, &parts[1..]).await?;
        }
        "get" => {
            handle_get_command(client, &parts[1..]).await?;
        }
        "getall" => {
            handle_getall_command(client, &parts[1..]).await?;
        }
        "search" => {
            handle_search_command(client, &parts[1..]).await?;
        }
        "stats" => {
            handle_stats_command(client).await?;
        }
        _ => {
            println!(
                "Unknown command: {}. Type 'help' for available commands.",
                parts[0]
            );
        }
    }

    Ok(())
}

async fn handle_intern_command(client: &mut TextBankClient<Channel>, args: &[&str]) -> Result<()> {
    if args.len() < 2 {
        println!("Usage: intern <lang> <text> OR intern <id> <lang> <text>");
        return Ok(());
    }

    let (text_id, lang, text) = if args.len() >= 3 {
        // Try to parse first arg as ID
        if let Ok(id) = args[0].parse::<u64>() {
            (id, args[1].to_string(), args[2..].join(" "))
        } else {
            // First arg is language
            (0, args[0].to_string(), args[1..].join(" "))
        }
    } else {
        // Two args: lang and text
        (0, args[0].to_string(), args[1..].join(" "))
    };

    let request = InternRequest {
        text: text.into_bytes(),
        lang,
        text_id,
    };

    match client.intern(request).await {
        Ok(response) => {
            let reply = response.into_inner();
            if reply.existed {
                println!("✓ Text already existed with ID: {}", reply.text_id);
            } else {
                println!("✓ Text added with ID: {}", reply.text_id);
            }
        }
        Err(e) => {
            eprintln!("✗ Failed to intern text: {}", e.message());
        }
    }

    Ok(())
}

async fn handle_get_command(client: &mut TextBankClient<Channel>, args: &[&str]) -> Result<()> {
    if args.is_empty() {
        println!("Usage: get <id> [lang]");
        return Ok(());
    }

    let text_id = args[0]
        .parse::<u64>()
        .context("Invalid text ID - must be a number")?;

    let lang = if args.len() > 1 {
        args[1].to_string()
    } else {
        String::new()
    };

    let request = GetRequest { text_id, lang };

    match client.get(request).await {
        Ok(response) => {
            let reply = response.into_inner();
            if reply.found {
                let text = String::from_utf8_lossy(&reply.text);
                println!("ID {}: \"{}\"", text_id, text);
            } else {
                println!("✗ No text found for ID: {}", text_id);
            }
        }
        Err(e) => {
            eprintln!("✗ Failed to get text: {}", e.message());
        }
    }

    Ok(())
}

async fn handle_getall_command(client: &mut TextBankClient<Channel>, args: &[&str]) -> Result<()> {
    if args.is_empty() {
        println!("Usage: getall <id>");
        return Ok(());
    }

    let text_id = args[0]
        .parse::<u64>()
        .context("Invalid text ID - must be a number")?;

    let request = GetAllRequest { text_id };

    match client.get_all(request).await {
        Ok(response) => {
            let reply = response.into_inner();
            if reply.translations.is_empty() {
                println!("✗ No translations found for ID: {}", text_id);
            } else {
                println!(
                    "✓ Found {} translation{} for ID {}:",
                    reply.translations.len(),
                    if reply.translations.len() == 1 {
                        ""
                    } else {
                        "s"
                    },
                    text_id
                );
                for translation in reply.translations {
                    let text = String::from_utf8_lossy(&translation.text);
                    println!("  {}: \"{}\"", translation.lang, text);
                }
            }
        }
        Err(e) => {
            eprintln!("✗ Failed to get all translations: {}", e.message());
        }
    }

    Ok(())
}

async fn handle_search_command(client: &mut TextBankClient<Channel>, args: &[&str]) -> Result<()> {
    if args.is_empty() {
        println!("Usage: search <pattern> [lang]");
        println!("Note: Use (?i) prefix for case-insensitive search");
        return Ok(());
    }

    let mut pattern = args[0].to_string();
    let lang = if args.len() > 1 {
        args[1].to_string()
    } else {
        String::new()
    };

    // Make search case-insensitive by default unless already specified
    if !pattern.starts_with("(?") {
        pattern = format!("(?i){}", pattern);
    }

    let request = SearchRequest { pattern, lang };

    match client.search(request).await {
        Ok(response) => {
            let reply = response.into_inner();
            if reply.hits.is_empty() {
                println!("✗ No matches found");
            } else {
                println!(
                    "✓ Found {} match{}:",
                    reply.hits.len(),
                    if reply.hits.len() == 1 { "" } else { "es" }
                );
                for hit in reply.hits {
                    let text = String::from_utf8_lossy(&hit.text);
                    println!("  ID {}: \"{}\"", hit.text_id, text);
                }
            }
        }
        Err(e) => {
            eprintln!("✗ Failed to search: {}", e.message());
        }
    }

    Ok(())
}

async fn handle_stats_command(client: &mut TextBankClient<Channel>) -> Result<()> {
    // Since the current TextBank service doesn't have a stats endpoint,
    // we'll provide some basic information by doing some test operations
    println!("TextBank Service Statistics:");
    println!("  Service: Connected ✓");
    println!("  Protocol: gRPC over HTTP/2");
    println!("  Address: 127.0.0.1:50051");

    // Try a simple operation to verify connectivity
    match client
        .get(tonic::Request::new(textbank::pb::GetRequest {
            text_id: 999999,
            lang: String::new(),
        }))
        .await
    {
        Ok(_) => println!("  Status: Service responding ✓"),
        Err(_) => println!("  Status: Service not responding ✗"),
    }

    Ok(())
}
