use srflow::{ExecutionError, Node};

pub struct AiNode {
    prompt: String,
    url: String,
}

impl AiNode {
    pub fn new(prompt: String, url: String) -> AiNode {
        AiNode { prompt, url }
    }
}

impl Node for AiNode {

    type Input = String;
    type Output = String;

    async fn run(&self, input: Self::Input) -> Result<String, ExecutionError> {
        
        Ok(input.to_string())
    }
}