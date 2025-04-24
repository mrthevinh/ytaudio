import requests
import json

url = "https://text.pollinations.ai/openai"
payload = {
    "model": "openai-audio",
    "messages": [
      {"role": "user", "content": "Sự khôn ngoan của Đạo: Mở khóa bí mật tối thượng của một cuộc sống cân bằng"}
    ],
    "voice": "onyx" # Choose voice
}
headers = {"Content-Type": "application/json"}
output_filename = "generated_audio_post.mp3"

try:
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    if 'audio/mpeg' in response.headers.get('Content-Type', ''):
        with open(output_filename, 'wb') as f:
            f.write(response.content)
        print(f"Audio saved successfully as {output_filename}")
    else:
        print("Error: Expected audio response, received:")
        print(f"Content-Type: {response.headers.get('Content-Type')}")
        print(response.text)
except requests.exceptions.RequestException as e:
    print(f"Error making TTS POST request: {e}")