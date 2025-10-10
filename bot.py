import requests

BOT_TOKEN = '8386936045:AAE18mrqRK9SxgDjj0nbglv5B5sxH1Waaxo'

def get_chat_id():
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        results = data.get("result", [])
        
        if not results:
            print("Heç bir mesaj tapılmadı. 📭 Zəhmət olmasa, botunuza Telegramdan bir mesaj göndərin və yenidən yoxlayın.")
        else:
            for update in results:
                message = update.get("message")
                if message:
                    chat = message.get("chat")
                    if chat:
                        print(f"✅ Chat ID: {chat.get('id')}")
                        print(f"📨 Mesaj: {message.get('text')}")
                else:
                    print("❗️ 'message' məlumatı tapılmadı.")
    else:
        print(f"Xəta baş verdi! Status kodu: {response.status_code}")

if __name__ == "__main__":
    get_chat_id()