# Standalone PowerON Bot

Окремий бот тільки для перегляду графіків відключень (без функціоналу керування ПК).

## Запуск

```bash
pip install -r requirements.txt
python -m playwright install chromium
```

Налаштуй токен:

- `POWERON_BOT_TOKEN`, або
- файл `poweron_bot_token.txt` у корені репозиторію (у репозиторії вже є шаблон `poweron_bot_token.txt.example` та порожній локальний `poweron_bot_token.txt` для вставки токена).

Опційно обмежити доступ:

- `POWERON_ALLOWED_IDS=123456,987654`

Старт:

```bash
python -m poweron_bot.main
```

## Можливості

- Покроковий пошук адреси: населений пункт → вулиця → будинок.
- Рендер скріншота фрагмента графіка з `poweron.toe.com.ua`.
- Історія останніх 3 адрес.
- Закріплення до 3 адрес для швидкого доступу.
