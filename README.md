# Standalone PowerON Bot

Окремий Telegram-бот для перегляду графіків відключень з `poweron.toe.com.ua`.

## Що вміє бот

- Покроковий пошук адреси: **населений пункт → вулиця → будинок**.
- Отримання та відправка скріншота фрагмента графіка відключень.
- Показ черг з API (ГПВ, ГАВ, АЧР, ГВСП, СГАВ).
- Історія останніх 3 адрес.
- Закріплення до 3 адрес для швидкого доступу.

## Структура репозиторію

```text
.
├── poweron_bot/
│   ├── __init__.py
│   ├── client.py
│   ├── main.py
│   └── wizard.py
├── poweron_bot_token.txt.example
├── requirements.txt
└── README.md
```

## Підготовка до запуску

### 1) Встанови залежності

```bash
pip install -r requirements.txt
python -m playwright install chromium
```

### 2) Налаштуй токен бота

Підтримуються 2 способи (пріоритет має змінна оточення):

1. Через ENV:

```bash
export POWERON_BOT_TOKEN="<YOUR_TELEGRAM_BOT_TOKEN>"
```

2. Через файл у корені репозиторію:

- Скопіюй шаблон `poweron_bot_token.txt.example` у `poweron_bot_token.txt`.
- Встав токен в **один рядок** без лапок.

> `poweron_bot_token.txt` має бути локальним файлом і не має комітитись у git.

### 3) (Опційно) обмеж доступ до бота

```bash
export POWERON_ALLOWED_IDS="123456,987654"
```

Якщо не задавати `POWERON_ALLOWED_IDS`, бот доступний усім, хто має доступ до нього в Telegram.

## Запуск

```bash
python -m poweron_bot.main
```

## Швидка перевірка перед запуском

```bash
python -m compileall poweron_bot
```

## Поширені проблеми

- **`Set POWERON_BOT_TOKEN or create poweron_bot_token.txt`**  
  Не задано токен через ENV і відсутній файл `poweron_bot_token.txt`.

- **Playwright не запускає Chromium**  
  Виконай: `python -m playwright install chromium`.

- **`ModuleNotFoundError` на залежностях**  
  Перевстанови пакети: `pip install -r requirements.txt`.
