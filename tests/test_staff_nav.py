from auth import ADMIN_COOKIE, READER_COOKIE, admin_serializer, reader_serializer, staff_nav_role


def test_nav_role_is_none_without_login_cookie():
    assert staff_nav_role({}) is None


def test_nav_role_ignores_forged_cookie():
    assert staff_nav_role({ADMIN_COOKIE: "forged-value"}) is None


def test_nav_role_from_signed_cookies():
    admin_token = admin_serializer.dumps({"id": 1, "email": "a@example.test"})
    reader_token = reader_serializer.dumps({"id": 2, "email": "r@example.test"})
    assert staff_nav_role({ADMIN_COOKIE: admin_token}) == "admin"
    assert staff_nav_role({READER_COOKIE: reader_token}) == "reader"
    assert staff_nav_role({ADMIN_COOKIE: admin_token, READER_COOKIE: reader_token}) == "admin"
