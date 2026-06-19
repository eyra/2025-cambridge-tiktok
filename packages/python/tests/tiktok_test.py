import pytest
import json
import zipfile
import io
from datetime import datetime, timedelta
from port.script import (
    extract_tiktok_data,
    ExtractionResult,
    get_json_data_from_file,
    parse_datetime,
)


# Helper functions
def create_test_zip(data):
    """Helper function to create a zip file in memory with test data"""
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.writestr("user_data.json", json.dumps(data))
    zip_buffer.seek(0)
    return zip_buffer


def get_recent_date(days_ago=30, **time_kwargs):
    """
    Generate a recent date string (within past 6 months) for testing.

    Args:
        days_ago: Number of days in the past (default: 30)
        **time_kwargs: Optional hour, minute, second overrides
    """
    base_date = datetime.now() - timedelta(days=days_ago)
    if time_kwargs:
        base_date = base_date.replace(**time_kwargs)
    return base_date.strftime("%Y-%m-%d %H:%M:%S")


def create_base_profile(username="testuser", likes_received=None):
    """Create a basic profile structure for testing."""
    profile_map = {"userName": username}
    if likes_received is not None:
        profile_map["likesReceived"] = likes_received
    return {"Profile": {"Profile Information": {"ProfileMap": profile_map}}}


def get_extraction_result_by_id(results, result_id):
    """Find an extraction result by its ID."""
    return next((r for r in results if r.id == result_id), None)


def assert_columns_exist(df, columns):
    """Assert that all specified columns exist in the dataframe."""
    assert all(col in df.columns for col in columns)


def create_full_test_data():
    """Create a complete test data structure with all sections."""
    return {
        **create_base_profile(likes_received=100),
        "Activity": {
            "Follower List": {"FansList": []},
            "Following List": {"Following": []},
            "Like List": {"ItemFavoriteList": []},
            "Video Browsing History": {"VideoList": []},
        },
        "Video": {"Videos": {"VideoList": []}},
        "Comment": {"Comments": {"CommentsList": []}},
        "Direct Messages": {"Chat History": {"ChatHistory": {}}},
    }


def test_extract_tiktok_data_empty_zip():
    """Test extraction with empty zip file"""
    empty_zip = create_test_zip({})
    result = extract_tiktok_data(empty_zip, "en")
    assert result == []


def test_extract_tiktok_data_valid_data():
    """Test extraction with valid TikTok data"""
    result = extract_tiktok_data(create_test_zip(create_full_test_data()), "en")

    assert len(result) > 0
    assert all(isinstance(item, ExtractionResult) for item in result)

    summary_data = get_extraction_result_by_id(result, "tiktok_summary")
    assert summary_data is not None
    assert summary_data.title is not None
    assert len(summary_data.data_frame) > 0


def test_extract_tiktok_data_with_messages():
    """Test extraction with direct messages data"""
    current_time = get_recent_date()
    test_data = {
        **create_base_profile(),
        "Direct Messages": {
            "Chat History": {
                "ChatHistory": {
                    "chat1": [
                        {"From": "testuser", "Date": current_time},
                        {"From": "otheruser", "Date": current_time},
                    ]
                }
            }
        },
    }

    result = extract_tiktok_data(create_test_zip(test_data), "en")

    messages_data = get_extraction_result_by_id(result, "tiktok_direct_messages")
    assert messages_data is not None
    assert len(messages_data.data_frame) == 2
    assert_columns_exist(messages_data.data_frame, ["Anonymous ID", "Sent"])


def test_extract_tiktok_data_invalid_json():
    """Test extraction with invalid JSON data"""
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.writestr("user_data.json", "invalid json")
    zip_buffer.seek(0)

    result = extract_tiktok_data(zip_buffer, "en")
    assert result == []


def test_extract_tiktok_data_with_video_posts():
    """Test extraction with video posts data"""
    test_data = {
        **create_base_profile(),
        "Video": {"Videos": {"VideoList": [{"Date": get_recent_date(), "Likes": "10"}]}},
    }

    result = extract_tiktok_data(create_test_zip(test_data), "en")

    video_data = get_extraction_result_by_id(result, "tiktok_posts")
    assert video_data is not None
    assert len(video_data.data_frame) > 0
    assert_columns_exist(video_data.data_frame, ["Videos", "Likes received"])


def test_extract_videos_viewed():
    """Test extraction of viewed videos data"""
    recent_date1 = get_recent_date(hour=15, minute=20, second=38)
    recent_date2 = get_recent_date(hour=18, minute=21, second=38)

    test_data = {
        **create_base_profile(),
        "Activity": {
            "Video Browsing History": {
                "VideoList": [
                    {"Date": recent_date1, "Link": "https://www.tiktokv.com/share/video/1111111111111111111/"},
                    {"Date": recent_date2, "Link": "https://www.tiktokv.com/share/video/2222222222222222222/"},
                ]
            }
        },
    }

    result = extract_tiktok_data(create_test_zip(test_data), "en")

    videos_viewed = get_extraction_result_by_id(result, "tiktok_videos_viewed")
    assert videos_viewed is not None
    assert len(videos_viewed.data_frame) == 2
    assert_columns_exist(videos_viewed.data_frame, ["Date", "Timeslot", "Link"])
    # Data is sorted newest first, so the 18:21 video should be first
    assert videos_viewed.data_frame.iloc[0]["Link"] == "https://www.tiktokv.com/share/video/2222222222222222222/"
    assert videos_viewed.data_frame.iloc[0]["Date"] == recent_date2


def test_extract_session_info():
    """Test extraction of session information"""
    test_data = {
        **create_base_profile(),
        "Activity": {
            "Video Browsing History": {
                "VideoList": [
                    {"Date": get_recent_date(hour=15, minute=20, second=38)},
                    {"Date": get_recent_date(hour=15, minute=21, second=38)},  # Same session
                    {"Date": get_recent_date(hour=18, minute=21, second=38)},  # New session (> 5 min gap)
                ]
            }
        },
    }

    result = extract_tiktok_data(create_test_zip(test_data), "en")

    session_info = get_extraction_result_by_id(result, "tiktok_session_info")
    assert session_info is not None
    assert len(session_info.data_frame) == 2  # Should have 2 sessions
    assert_columns_exist(session_info.data_frame, ["Start", "Duration (in minutes)"])


def test_extract_comments_and_likes():
    """Test extraction of comments and likes data"""
    recent_date = get_recent_date()

    test_data = {
        **create_base_profile(),
        "Comment": {
            "Comments": {
                "CommentsList": [
                    {"Date": recent_date, "comment": "Great post! 📚", "photo": "N/A", "url": ""}
                ]
            }
        },
        "Activity": {
            "Like List": {"ItemFavoriteList": [{"Date": recent_date}]}
        },
    }

    result = extract_tiktok_data(create_test_zip(test_data), "en")

    comments_likes = get_extraction_result_by_id(result, "tiktok_comments_and_likes")
    assert comments_likes is not None
    assert_columns_exist(comments_likes.data_frame, ["Date", "Timeslot", "Comment posts", "Likes given"])
    assert comments_likes.data_frame["Comment posts"].sum() > 0
    assert comments_likes.data_frame["Likes given"].sum() > 0


def test_extract_direct_messages():
    """Test extraction of direct messages"""
    test_data = {
        **create_base_profile(),
        "Direct Messages": {
            "Chat History": {
                "ChatHistory": {
                    "chat1": [
                        {"From": "testuser", "Date": get_recent_date(hour=15, minute=20, second=38)},
                        {"From": "otheruser", "Date": get_recent_date(hour=15, minute=21, second=38)},
                    ]
                }
            }
        },
    }

    result = extract_tiktok_data(create_test_zip(test_data), "en")

    messages = get_extraction_result_by_id(result, "tiktok_direct_messages")
    assert messages is not None
    assert len(messages.data_frame) == 2
    assert_columns_exist(messages.data_frame, ["Anonymous ID", "Sent"])
    # Check that testuser (the owner) gets ID 1
    assert 1 in messages.data_frame["Anonymous ID"].values


def test_extract_tiktok_data_alternate_profile_structure():
    """Test extraction with alternate profile structure (Profile Info vs Profile Information)"""
    test_data = {
        "Profile": {
            "Profile Info": {
                "ProfileMap": {"userName": "testuser", "likesReceived": 250}
            }
        },
        "Activity": {
            "Follower List": {"FansList": []},
            "Following List": {"Following": []},
            "Like List": {"ItemFavoriteList": []},
            "Video Browsing History": {"VideoList": []},
        },
        "Video": {"Videos": {"VideoList": []}},
        "Comment": {"Comments": {"CommentsList": []}},
        "Direct Messages": {"Chat History": {"ChatHistory": {}}},
    }

    test_zip = create_test_zip(test_data)
    result = extract_tiktok_data(test_zip, "en")

    # Check if summary data is present and contains the likes
    summary_data = next((r for r in result if r.id == "tiktok_summary"), None)
    assert summary_data is not None
    assert summary_data.title is not None
    assert len(summary_data.data_frame) > 0

    # Verify that likes received from alternate structure are correctly extracted
    likes_row = summary_data.data_frame[summary_data.data_frame["Description"] == "Likes received"]
    assert len(likes_row) == 1
    assert likes_row.iloc[0]["Number"] == 250


def test_extract_tiktok_data_with_locale():
    """Test extraction with different locales to verify translation functionality"""
    test_data = create_full_test_data()

    # Test locales with expected translations
    locale_tests = [
        ("en", "Followers", "Videos published"),
        ("de", "Follower", "Veröffentlichte Videos"),
        ("it", "Follower", "Video pubblicati"),
        ("nl", "Volgers", "Gepubliceerde video's"),
    ]

    for locale, expected_followers, expected_videos in locale_tests:
        result = extract_tiktok_data(create_test_zip(test_data), locale)
        summary = get_extraction_result_by_id(result, "tiktok_summary")
        assert expected_followers in summary.data_frame["Description"].values
        assert expected_videos in summary.data_frame["Description"].values

    # Test fallback to English for unsupported locale
    result_unsupported = extract_tiktok_data(create_test_zip(test_data), "fr")
    summary_unsupported = get_extraction_result_by_id(result_unsupported, "tiktok_summary")
    assert "Followers" in summary_unsupported.data_frame["Description"].values


def test_get_json_data_from_file_with_zip_file_like_object():
    """Test that get_json_data_from_file correctly detects zip files without loading entire file as JSON"""
    test_data = create_base_profile()
    result = get_json_data_from_file(create_test_zip(test_data))

    assert len(result) == 1
    assert result[0]["Profile"]["Profile Information"]["ProfileMap"]["userName"] == "testuser"


def test_get_json_data_from_file_with_json_file_like_object():
    """Test that get_json_data_from_file correctly handles plain JSON file-like objects"""
    test_data = create_base_profile()
    json_buffer = io.StringIO(json.dumps(test_data))

    result = get_json_data_from_file(json_buffer)

    assert len(result) == 1
    assert result[0]["Profile"]["Profile Information"]["ProfileMap"]["userName"] == "testuser"


def test_get_json_data_from_file_with_large_zip_memory_efficiency():
    """Test that large zip files don't get loaded into memory as JSON first"""
    test_data = {
        **create_base_profile(),
        "Activity": {
            "Video Browsing History": {
                "VideoList": [
                    {"Date": get_recent_date(), "Link": f"https://example.com/video/{i}"}
                    for i in range(1000)
                ]
            }
        }
    }

    result = get_json_data_from_file(create_test_zip(test_data))

    assert len(result) == 1
    assert result[0]["Profile"]["Profile Information"]["ProfileMap"]["userName"] == "testuser"
    assert len(result[0]["Activity"]["Video Browsing History"]["VideoList"]) == 1000


def test_get_json_data_from_file_with_invalid_file():
    """Test that get_json_data_from_file handles invalid files gracefully"""
    invalid_buffer = io.BytesIO(b"This is not JSON or a ZIP file")

    result = get_json_data_from_file(invalid_buffer)

    assert result == []


def test_get_json_data_from_file_with_empty_zip():
    """Test that get_json_data_from_file handles empty zip files"""
    empty_zip = io.BytesIO()
    with zipfile.ZipFile(empty_zip, "w", zipfile.ZIP_DEFLATED) as zf:
        pass  # Create empty zip
    empty_zip.seek(0)

    result = get_json_data_from_file(empty_zip)

    assert result == []


def test_parse_datetime_without_utc_suffix():
    """Legacy TikTok export format (no timezone suffix)."""
    assert parse_datetime("2025-04-23 10:26:35") == datetime(2025, 4, 23, 10, 26, 35)


def test_parse_datetime_with_utc_suffix():
    """TikTok export format from mid-2026 onwards includes a ' UTC' suffix."""
    assert parse_datetime("2025-04-23 10:26:35 UTC") == datetime(2025, 4, 23, 10, 26, 35)


def test_parse_datetime_unknown_format_raises():
    with pytest.raises(ValueError, match="Unrecognized TikTok datetime format"):
        parse_datetime("23/04/2025 10:26:35")
