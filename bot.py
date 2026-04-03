import asyncio
import logging
import os
from pathlib import Path
from urllib.parse import urlparse

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    CallbackQuery,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from dotenv import load_dotenv

from database import Database, period_start_iso


load_dotenv()


BOT_TOKEN = os.getenv("BOT_TOKEN")
if BOT_TOKEN is None:
    raise ValueError("BOT_TOKEN not found in environment variables.")

raw_channel_id = os.getenv("CHANNEL_ID")
if raw_channel_id is None:
    raise ValueError("CHANNEL_ID not found in environment variables.")

try:
    CHANNEL_ID = int(raw_channel_id)
except ValueError as exc:
    raise ValueError(f"CHANNEL_ID must be an integer, got {raw_channel_id!r}") from exc

raw_admin_id = os.getenv("ADMIN_ID")
ADMIN_ID = None
if raw_admin_id:
    try:
        ADMIN_ID = int(raw_admin_id)
    except ValueError as exc:
        raise ValueError(f"ADMIN_ID must be an integer, got {raw_admin_id!r}") from exc

DATABASE_PATH = os.getenv("DATABASE_PATH", "bot_data.sqlite3")


LEAD_MAGNET_FILE = "poimi-svoi-son-za-20-minut.pdf"
LEAD_MAGNET_CAPTION = (
    "Вот твой PDF!\n"
    "Сон может сказать о жизни больше, чем неделя размышлений.\n"
    "В этом файле простой способ быстро понять его смысл."
)
WELCOME_TEXT = (
    "<b>Привет!</b>\n\n"
    "Чтобы получить чек-лист, нужно быть подписчиком моего канала ниже."
)
CHANNEL_LINK = "https://t.me/masterkey444"
EXPORT_FILE = "users_export.csv"
MAX_BUTTON_TEXT_LENGTH = 22


logging.basicConfig(level=logging.INFO)

bot = Bot(
    token=BOT_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML),
)

dp = Dispatcher()
router = Router()
dp.include_router(router)
db = Database(DATABASE_PATH)
broadcast_states: dict[int, dict] = {}


def get_subscribe_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Подписаться на канал", url=CHANNEL_LINK)],
            [InlineKeyboardButton(text="Я подписан", callback_data="check_sub")],
        ]
    )


def get_broadcast_confirm_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Тест себе", callback_data="broadcast_test")],
            [InlineKeyboardButton(text="Отправить", callback_data="broadcast_confirm")],
            [InlineKeyboardButton(text="Отмена", callback_data="broadcast_cancel")],
        ]
    )


def get_broadcast_button_choice_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Добавить кнопку", callback_data="broadcast_add_button")],
            [InlineKeyboardButton(text="Без кнопки", callback_data="broadcast_no_button")],
            [InlineKeyboardButton(text="Отмена", callback_data="broadcast_cancel")],
        ]
    )


def is_admin(user_id: int) -> bool:
    return ADMIN_ID is not None and user_id == ADMIN_ID


def build_stats_block(title: str, stats: dict[str, int]) -> str:
    unique_users = stats["unique_users"]
    start_total = stats["start_total"]
    subscription_verified = stats["subscription_verified"]
    lead_magnet_sent = stats["lead_magnet_sent"]

    subscribe_conversion = (subscription_verified / unique_users * 100) if unique_users else 0
    lead_magnet_conversion = (lead_magnet_sent / unique_users * 100) if unique_users else 0

    return (
        f"<b>{title}</b>\n"
        f"Уникальных пользователей: <b>{unique_users}</b>\n"
        f"Нажали /start: <b>{start_total}</b>\n"
        f"Прошли проверку подписки: <b>{subscription_verified}</b>\n"
        f"Получили лидмагнит: <b>{lead_magnet_sent}</b>\n\n"
        f"Конверсия в подписку: <b>{subscribe_conversion:.1f}%</b>\n"
        f"Конверсия в лидмагнит: <b>{lead_magnet_conversion:.1f}%</b>"
    )


async def is_user_subscribed(user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(chat_id=CHANNEL_ID, user_id=user_id)
        return member.status in {"member", "administrator", "creator"}
    except Exception as exc:
        logging.error("Subscription check failed for %s: %s", user_id, exc)
        return False


def clear_broadcast_state(user_id: int) -> None:
    broadcast_states.pop(user_id, None)


def set_broadcast_state(user_id: int, **values) -> None:
    state = broadcast_states.setdefault(user_id, {})
    state.update(values)


def get_broadcast_markup(state: dict) -> InlineKeyboardMarkup | None:
    button_text = state.get("button_text")
    button_url = state.get("button_url")
    if not button_text or not button_url:
        return None

    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=button_text, url=button_url)],
        ]
    )


def is_valid_button_url(value: str) -> bool:
    parsed = urlparse(value)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def is_valid_button_text(value: str) -> bool:
    return 1 <= len(value.strip()) <= MAX_BUTTON_TEXT_LENGTH


async def show_broadcast_preview(chat_id: int, state: dict) -> None:
    recipient_count = len(db.get_broadcast_recipients())
    await bot.send_message(
        chat_id=chat_id,
        text=(
            "<b>Предпросмотр рассылки</b>\n\n"
            f"Получателей сейчас: <b>{recipient_count}</b>"
        ),
    )

    reply_markup = get_broadcast_markup(state)
    if state["content_type"] == "photo":
        await bot.send_photo(
            chat_id=chat_id,
            photo=state["photo_file_id"],
            caption=state.get("caption") or "",
            reply_markup=reply_markup,
        )
    else:
        await bot.send_message(
            chat_id=chat_id,
            text=state["text"],
            reply_markup=reply_markup,
        )

    await bot.send_message(
        chat_id=chat_id,
        text="Подтверди отправку или отмени ее.",
        reply_markup=get_broadcast_confirm_keyboard(),
    )


async def send_broadcast_content(chat_id: int, state: dict) -> None:
    reply_markup = get_broadcast_markup(state)
    if state["content_type"] == "photo":
        await bot.send_photo(
            chat_id=chat_id,
            photo=state["photo_file_id"],
            caption=state.get("caption") or "",
            reply_markup=reply_markup,
        )
        return

    await bot.send_message(
        chat_id=chat_id,
        text=state["text"],
        reply_markup=reply_markup,
    )


@router.message(CommandStart())
async def cmd_start(message: Message) -> None:
    user = message.from_user
    db.record_start(
        telegram_id=user.id,
        username=user.username,
        first_name=user.first_name,
    )

    if await is_user_subscribed(user.id):
        db.record_subscription_verified(user.id)
        await send_lead_magnet(message)
        return

    await message.answer(
        WELCOME_TEXT,
        reply_markup=get_subscribe_keyboard(),
        disable_web_page_preview=True,
    )


@router.callback_query(F.data == "check_sub")
async def process_check_sub(call: CallbackQuery) -> None:
    await call.answer()

    user = call.from_user
    db.upsert_user(
        telegram_id=user.id,
        username=user.username,
        first_name=user.first_name,
    )

    if await is_user_subscribed(user.id):
        db.record_subscription_verified(user.id)
        if call.message:
            await call.message.edit_text("Подписка найдена. Отправляю файл...")
            await send_lead_magnet(call.message)
        return

    if call.message:
        try:
            await call.message.delete()
        except Exception:
            pass

        await call.message.answer(
            "Ты еще не подписан на канал.\n\n"
            "Подпишись по кнопке ниже и нажми «Я подписан» снова.",
            reply_markup=get_subscribe_keyboard(),
            disable_web_page_preview=True,
        )

    await call.answer(
        "Подписка не найдена. Проверь, что ты действительно подписался.",
        show_alert=True,
    )


@router.message(Command("stats"))
async def cmd_stats(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer("У тебя нет доступа к этой команде.")
        return

    today_stats = db.get_period_stats(period_start_iso(1))
    week_stats = db.get_period_stats(period_start_iso(7))
    all_time_stats = db.get_period_stats()

    stats_text = (
        "<b>Статистика бота</b>\n\n"
        f"{build_stats_block('Сегодня', today_stats)}\n\n"
        f"{build_stats_block('Последние 7 дней', week_stats)}\n\n"
        f"{build_stats_block('Все время', all_time_stats)}"
    )
    await message.answer(stats_text)


@router.message(Command("export_users"))
async def cmd_export_users(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer("У тебя нет доступа к этой команде.")
        return

    export_path = db.export_users_csv(EXPORT_FILE)
    await message.answer_document(
        document=FSInputFile(export_path),
        caption="Экспорт пользователей готов.",
    )


@router.message(Command("broadcast"))
async def cmd_broadcast(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer("У тебя нет доступа к этой команде.")
        return

    set_broadcast_state(message.from_user.id, stage="awaiting_content")
    await message.answer(
        "Пришли содержимое рассылки одним сообщением.\n\n"
        "Можно отправить:\n"
        "- обычный текст\n"
        "- фото с подписью"
    )


@router.message(F.photo, lambda message: is_admin(message.from_user.id))
async def process_broadcast_photo(message: Message) -> None:
    state = broadcast_states.get(message.from_user.id)
    if not state or state.get("stage") != "awaiting_content":
        return

    photo = message.photo[-1]
    set_broadcast_state(
        message.from_user.id,
        stage="awaiting_button_choice",
        content_type="photo",
        photo_file_id=photo.file_id,
        caption=message.caption or "",
        text=None,
        button_text=None,
        button_url=None,
    )
    await message.answer(
        "Хочешь добавить кнопку под рассылкой?",
        reply_markup=get_broadcast_button_choice_keyboard(),
    )


@router.message(F.text, lambda message: is_admin(message.from_user.id))
async def process_broadcast_text(message: Message) -> None:
    if message.text.startswith("/"):
        return

    state = broadcast_states.get(message.from_user.id)
    if not state:
        return

    stage = state.get("stage")
    if stage == "awaiting_content":
        set_broadcast_state(
            message.from_user.id,
            stage="awaiting_button_choice",
            content_type="text",
            text=message.text,
            photo_file_id=None,
            caption=None,
            button_text=None,
            button_url=None,
        )
        await message.answer(
            "Хочешь добавить кнопку под рассылкой?",
            reply_markup=get_broadcast_button_choice_keyboard(),
        )
        return

    if stage == "awaiting_button_value":
        parts = [part.strip() for part in message.text.split("|", maxsplit=1)]
        if len(parts) != 2 or not parts[0] or not is_valid_button_url(parts[1]):
            await message.answer(
                "Не понял формат кнопки. Пришли так:\n"
                "Текст кнопки | https://example.com"
            )
            return

        if not is_valid_button_text(parts[0]):
            await message.answer(
                f"Текст кнопки лучше сделать короче, до {MAX_BUTTON_TEXT_LENGTH} символов, "
                "чтобы он не обрезался в Telegram.\n\n"
                "Пример:\n"
                "Записаться | https://example.com"
            )
            return

        set_broadcast_state(
            message.from_user.id,
            stage="preview",
            button_text=parts[0],
            button_url=parts[1],
        )
        await show_broadcast_preview(message.chat.id, broadcast_states[message.from_user.id])
        return


@router.callback_query(F.data == "broadcast_add_button")
async def process_broadcast_add_button(call: CallbackQuery) -> None:
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа.", show_alert=True)
        return

    state = broadcast_states.get(call.from_user.id)
    if not state or state.get("stage") != "awaiting_button_choice":
        await call.answer("Черновик рассылки не найден.", show_alert=True)
        return

    set_broadcast_state(call.from_user.id, stage="awaiting_button_value")
    await call.answer()
    if call.message:
        await call.message.edit_reply_markup(reply_markup=None)
        await call.message.answer(
            "Пришли кнопку в формате:\n"
            "Текст кнопки | https://example.com"
        )


@router.callback_query(F.data == "broadcast_no_button")
async def process_broadcast_no_button(call: CallbackQuery) -> None:
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа.", show_alert=True)
        return

    state = broadcast_states.get(call.from_user.id)
    if not state or state.get("stage") != "awaiting_button_choice":
        await call.answer("Черновик рассылки не найден.", show_alert=True)
        return

    set_broadcast_state(
        call.from_user.id,
        stage="preview",
        button_text=None,
        button_url=None,
    )
    await call.answer()
    if call.message:
        await call.message.edit_reply_markup(reply_markup=None)
    await show_broadcast_preview(call.from_user.id, broadcast_states[call.from_user.id])


@router.callback_query(F.data == "broadcast_cancel")
async def process_broadcast_cancel(call: CallbackQuery) -> None:
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа.", show_alert=True)
        return

    clear_broadcast_state(call.from_user.id)
    await call.answer("Рассылка отменена.")
    if call.message:
        await call.message.edit_reply_markup(reply_markup=None)
        await call.message.answer("Рассылка отменена.")


@router.callback_query(F.data == "broadcast_test")
async def process_broadcast_test(call: CallbackQuery) -> None:
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа.", show_alert=True)
        return

    state = broadcast_states.get(call.from_user.id)
    if not state or state.get("stage") != "preview":
        await call.answer("Черновик рассылки не найден.", show_alert=True)
        return

    await call.answer("Отправляю тест.")
    await send_broadcast_content(call.from_user.id, state)
    if call.message:
        await call.message.answer(
            "Тест отправлен тебе. Если все выглядит хорошо, нажми `Отправить` для массовой рассылки или `Отмена`.",
        )


@router.callback_query(F.data == "broadcast_confirm")
async def process_broadcast_confirm(call: CallbackQuery) -> None:
    if not is_admin(call.from_user.id):
        await call.answer("Нет доступа.", show_alert=True)
        return

    state = broadcast_states.get(call.from_user.id)
    if not state or state.get("stage") != "preview":
        await call.answer("Черновик рассылки не найден.", show_alert=True)
        return

    clear_broadcast_state(call.from_user.id)
    await call.answer("Запускаю рассылку.")
    if call.message:
        await call.message.edit_reply_markup(reply_markup=None)
        await call.message.answer("Рассылка запущена. Это может занять немного времени.")

    recipients = db.get_broadcast_recipients()
    sent_count = 0
    blocked_count = 0
    failed_count = 0

    for telegram_id in recipients:
        try:
            await send_broadcast_content(telegram_id, state)
            db.record_broadcast_result(telegram_id, "sent")
            sent_count += 1
        except TelegramForbiddenError as exc:
            db.record_broadcast_result(telegram_id, "blocked", str(exc))
            blocked_count += 1
        except TelegramBadRequest as exc:
            db.record_broadcast_result(telegram_id, "failed", str(exc))
            failed_count += 1
        except Exception as exc:
            db.record_broadcast_result(telegram_id, "failed", str(exc))
            failed_count += 1

    report_text = (
        "<b>Рассылка завершена</b>\n\n"
        f"Отправлено: <b>{sent_count}</b>\n"
        f"Заблокировали бота: <b>{blocked_count}</b>\n"
        f"Ошибок: <b>{failed_count}</b>"
    )
    await bot.send_message(chat_id=call.from_user.id, text=report_text)


async def send_lead_magnet(target: Message) -> None:
    file_path = Path(LEAD_MAGNET_FILE)
    if not file_path.is_file():
        await target.answer(
            "Ошибка: файл не найден на сервере.\nСвяжись с владельцем бота."
        )
        return

    try:
        await target.answer_document(
            document=FSInputFile(file_path),
            caption=LEAD_MAGNET_CAPTION,
        )
        db.record_lead_magnet_sent(target.chat.id)
    except Exception as exc:
        logging.error("Lead magnet send failed for %s: %s", target.chat.id, exc)
        db.record_delivery_failure(target.chat.id, str(exc))
        await target.answer(
            "Не удалось отправить файл. Попробуй позже или напиши администратору."
        )


async def main() -> None:
    db.init()
    try:
        await dp.start_polling(
            bot,
            allowed_updates=["message", "callback_query"],
            drop_pending_updates=True,
        )
    except Exception as exc:
        logging.error("Critical polling error: %s", exc)
    finally:
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
