# フォームから取得
style = request.form.get("style", "general")

# meta に追加
meta = {
    "astrology_system": astrology_system,
    "theme": theme,
    "message": user_message,
    "style": style,
}
