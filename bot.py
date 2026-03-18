import asyncio
import logging
from pathlib import Path

from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import CommandStart
from aiogram.types import Message, FSInputFile, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery

# Новые импорты для правильной работы parse_mode в новых версиях aiogram
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

# ──────────────────────────────────────────────
# Твои настройки
# ──────────────────────────────────────────────

BOT_TOKEN = "8655695766:AAE2COF5mDDkCS0tx6I0fCKCe2d_ljcMyNA"

# Правильный ID канала (ты подтвердил)
CHANNEL_ID = -1002005439356

# Путь к PDF (положи файл рядом с bot.py и переименуй или измени путь)
LEAD_MAGNET_FILE = "poimi-svoi-son-za-20-minut.pdf"

LEAD_MAGNET_CAPTION = (
    "Вот твой PDF! 🎁\n"
    "Сон может сказать о жизни больше, чем неделя размышлений \n"
    "В этом файле — простой способ быстро понять его смысл \n\n"
    
)

WELCOME_TEXT = (
    "<b>Привет!</b>\n\n"
    "Чтобы получить Чек-лист — нужно быть подписчиком моего канала 👇\n"
    
)

CHANNEL_LINK = "https://t.me/masterkey444"

# ──────────────────────────────────────────────

logging.basicConfig(level=logging.INFO)

# Правильная инициализация бота с parse_mode=HTML по умолчанию
bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML)
)

dp = Dispatcher()
router = Router()
dp.include_router(router)


def get_subscribe_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔥 Подписаться на канал", url=CHANNEL_LINK)],
        [InlineKeyboardButton(text="✅ Я подписан", callback_data="check_sub")]
    ])


async def is_user_subscribed(user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(chat_id=CHANNEL_ID, user_id=user_id)
       # print(f"User {user_id} → статус в канале: {member.status}")
        return member.status in {"member", "administrator", "creator"}
    except Exception as e:
       # print(f"Ошибка get_chat_member: {e}")
        logging.error(f"Ошибка проверки подписки для {user_id}: {e}")
        return False


@router.message(CommandStart())
async def cmd_start(message: Message):
    if await is_user_subscribed(message.from_user.id):
        await send_lead_magnet(message)
    else:
        await message.answer(
            WELCOME_TEXT,
            reply_markup=get_subscribe_keyboard(),
            disable_web_page_preview=True
        )


@router.callback_query(F.data == "check_sub")
async def process_check_sub(call: CallbackQuery):
    await call.answer()  # убираем "часики"

    user_id = call.from_user.id
    subscribed = await is_user_subscribed(user_id)

    if subscribed:
        # Подписан → редактируем сообщение и отправляем файл
        await call.message.edit_text("Подписка найдена! Отправляю файл...")
        await send_lead_magnet(call.message)
    else:
        # НЕ подписан → удаляем старое сообщение и отправляем новое
        try:
            await call.message.delete()
        except Exception:
            pass  # если сообщение уже удалено или нет прав — игнорируем

        await call.message.answer(
            "Ты ещё не подписан на канал 😕\n\n"
            "Подпишись пожалуйста по кнопке ниже и нажми «Я подписан» снова!",
            reply_markup=get_subscribe_keyboard(),
            disable_web_page_preview=True
        )

        # Дополнительно показываем всплывающее уведомление
        await call.answer(
            "Подписка не найдена! Проверь, подписался ли ты.",
            show_alert=True
        )


async def send_lead_magnet(target: Message):
    file_path = Path(LEAD_MAGNET_FILE)
    if not file_path.is_file():
        await target.answer("Ошибка: файл не найден на сервере 😢\nСвяжитесь с владельцем бота.")
        return

    try:
        await target.answer_document(
            document=FSInputFile(file_path),
            caption=LEAD_MAGNET_CAPTION
        )
    except Exception as e:
        logging.error(f"Ошибка отправки файла: {e}")
        await target.answer("Не удалось отправить файл. Попробуй позже или напиши админу.")


async def main():
    try:
        await dp.start_polling(
            bot,
            allowed_updates=["message", "callback_query"],
            drop_pending_updates=True
        )
    finally:
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())