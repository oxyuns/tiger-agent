from ollama import Client

client = Client(host='http://localhost:11434')
response = client.chat(
    model='deepseek-r1:7b',
    messages=[
        {'role': 'system', 'content': 'You are a confident expert content classifier for blockchain and digital assets. Answer with <think>your analysis</think> followed by ONLY "YES" or "NO".'},
        {'role': 'user', 'content': 'News Title: Can Cardano and Toncoin Hit New Highs as Crypto Whales Gravitate to JetBolt'}
    ]
)
answer = response.message.content.split('</think>')[-1].strip()
print(answer)