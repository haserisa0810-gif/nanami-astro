import re
from typing import Iterable


_FULLWIDTH_PUNCT_MAP = str.maketrans({
    '．': '。',
    '，': '、',
    '｡': '。',
    '､': '、',
})


def _apply_patterns(text: str, patterns: Iterable[tuple[str, str]]) -> str:
    for pattern, replacement in patterns:
        text = re.sub(pattern, replacement, text)
    return text


def fix_punctuation(text: str) -> str:
    """句読点まわりの不自然さを軽く補正する。"""
    if not text:
        return ''

    text = str(text).translate(_FULLWIDTH_PUNCT_MAP)
    text = re.sub(r'\*\*', '', text)
    patterns = [
        (r'([をにもへや])、', r'\1'),
        (r'(こと|もの|ところ|ため|はず|わけ|よう)が、', r'\1が'),
        (r'(こと|もの)の、', r'\1の'),
        (r'([ぁ-んァ-ン一-龥])\s+、', r'\1、'),
        (r'、{2,}', '、'),
        (r'。{2,}', '。'),
        (r'、。', '。'),
        (r'。 、', '。'),
        (r'([？！])。', r'\1'),
    ]
    return _apply_patterns(text, patterns)


def humanize_text(text: str) -> str:
    """AIっぽい言い回しを軽く崩して読みやすくする。"""
    if not text:
        return ''

    patterns = [
        (r'、と感じていませんか？', 'でしょう。'),
        (r'、と感じていませんか\?', 'でしょう。'),
        (r'と感じていませんか？', 'でしょう。'),
        (r'と感じていませんか\?', 'でしょう。'),
        (r'かもしれません。', 'でしょう。'),

        (r'しかし、最近、', 'ただ、最近は'),
        (r'しかし、', 'ただ、'),

        (r'ようにあなたは', 'ことから、あなたは'),
        (r'([一-龥ぁ-んァ-ンA-Za-z0-9])ように、?あなたは', r'\1ことから、あなたは'),
        (r'あなたは本来、あなたは', 'あなたは'),
        (r'あなたは本来、', 'あなたは'),
        (r'本来、', ''),
        (r'少しズレが生じているのかもしれません。', '少しズレが生じているでしょう。'),
        (r'〜しやすい', '傾向があります'),
        (r'([^\n。！？]{90,}?)、(?!\n)', r'\1、\n'),
        (r'。(?=[^\n])', '。\n'),
        (r'([！？])(?=[^\n])', r'\1\n'),
        (r'[ \t]+', ' '),
        (r'\n{3,}', '\n\n'),
    ]
    text = _apply_patterns(text, patterns)

    text = re.sub(
        r'([。\n]|^)([^\n。]{0,40})が([^\n。]{0,30})ことから、あなたは',
        r'\1\2が\3。\nそのことから、あなたは',
        text,
    )
    return text.strip()


def fix_sentence_endings(text: str) -> str:
    """名詞止めっぽい不自然な終わり方を軽く補正する。"""
    if not text:
        return ''

    patterns = [
        (r'時期。', '時期です。'),
        (r'傾向。', '傾向があります。'),
        (r'可能性。', '可能性があります。'),
        (r'流れ。', '流れです。'),
    ]
    return _apply_patterns(text, patterns)


def trim_unnecessary(text: str) -> str:
    """よくある余計な締め文を削る。"""
    if not text:
        return ''

    patterns = [
        (r'ここから先は、?仕事・恋愛・人間関係なども含めて、?もう少し具体的に読み解くことができます。?', ''),
        (r'ここから先は.*', ''),
        (r'さらに詳しく.*', ''),
        (r'ご希望であれば.*', ''),
    ]
    return _apply_patterns(text, patterns).strip()


def normalize_layout(text: str) -> str:
    if not text:
        return ''

    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'[ \t]+\n', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def format_ai_text(text: str) -> str:
    text = fix_punctuation(text)
    text = humanize_text(text)
    text = fix_sentence_endings(text)
    text = trim_unnecessary(text)
    text = normalize_layout(text)
    return text
