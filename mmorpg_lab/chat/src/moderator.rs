use aho_corasick::{AhoCorasick};
use core::panic;
use std::fs;
use tracing::{error, info};

pub struct Moderator {
    automaton: AhoCorasick,
}

impl Moderator {
    pub fn new(filepath: &str) -> Self {
        // Load bad words from the specified file
        let content = fs::read_to_string(filepath).expect("Could not read badwords.txt");
        let bad_words: Vec<String> = content
            .lines()
            .map(|line| line.trim().to_lowercase())
            .filter(|line| !line.is_empty())
            .collect();

        // Build the Aho-Corasick automaton with the loaded bad words
        if let Ok(automaton) = AhoCorasick::builder()
            .ascii_case_insensitive(true)
            .build(bad_words)
        {
            info!("Successfully built Aho-Corasick automaton");
            Self { automaton }
        } else {
            error!("Could not build Aho-Corasick automaton");
            panic!("Failed to initialize Moderator");
        }
    }

    pub fn moderate_message(&self, message: &str) -> String {
        let normalized = message.to_lowercase();
        self.automaton.replace_all(&normalized, &["*"; 100])
    }
}
