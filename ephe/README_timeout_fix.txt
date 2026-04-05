修正内容:
- 相性鑑定(analysis_type=compatibility)では /analyze 内で transit_data を先計算しない
- 相性本文を先に返し、重いトランジット計算は別リクエスト側へ逃がす前提

目的:
- 相性鑑定 + トランジットON での upstream request timeout を避ける
