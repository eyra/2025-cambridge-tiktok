#!/usr/bin/env python
"""Test script to verify summary counts include filtered data"""

import json
import zipfile
import io
from datetime import datetime, timedelta
import pytest
from port.script import extract_tiktok_data


def create_test_zip_with_mixed_dates():
    """Create a test zip with data from various time periods"""
    base_date = datetime.now()

    test_data = {
        "Profile": {
            "Profile Information": {
                "ProfileMap": {
                    "userName": "testuser",
                    "likesReceived": 1000
                }
            }
        },
        "Activity": {
            "Video Browsing History": {
                "VideoList": [
                    # Recent videos (past 6 months)
                    {"Date": (base_date - timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S"), "Link": "https://example.com/1"},
                    {"Date": (base_date - timedelta(days=60)).strftime("%Y-%m-%d %H:%M:%S"), "Link": "https://example.com/2"},
                    {"Date": (base_date - timedelta(days=90)).strftime("%Y-%m-%d %H:%M:%S"), "Link": "https://example.com/3"},
                    # Old videos (more than 6 months)
                    {"Date": (base_date - timedelta(days=200)).strftime("%Y-%m-%d %H:%M:%S"), "Link": "https://example.com/4"},
                    {"Date": (base_date - timedelta(days=250)).strftime("%Y-%m-%d %H:%M:%S"), "Link": "https://example.com/5"},
                ]
            },
            "Like List": {
                "ItemFavoriteList": [
                    # Recent likes (past 6 months)
                    {"Date": (base_date - timedelta(days=10)).strftime("%Y-%m-%d %H:%M:%S")},
                    {"Date": (base_date - timedelta(days=50)).strftime("%Y-%m-%d %H:%M:%S")},
                    # Old likes (more than 6 months)
                    {"Date": (base_date - timedelta(days=200)).strftime("%Y-%m-%d %H:%M:%S")},
                ]
            }
        },
        "Comment": {
            "Comments": {
                "CommentsList": [
                    # Recent comments (past 6 months)
                    {"Date": (base_date - timedelta(days=20)).strftime("%Y-%m-%d %H:%M:%S"), "comment": "Comment 1"},
                    {"Date": (base_date - timedelta(days=40)).strftime("%Y-%m-%d %H:%M:%S"), "comment": "Comment 2"},
                    # Old comments (more than 6 months)
                    {"Date": (base_date - timedelta(days=220)).strftime("%Y-%m-%d %H:%M:%S"), "comment": "Comment 3"},
                ]
            }
        },
        "Video": {
            "Videos": {
                "VideoList": [
                    # Recent posts (past 6 months)
                    {"Date": (base_date - timedelta(days=15)).strftime("%Y-%m-%d %H:%M:%S"), "Likes": "100"},
                    # Old posts (more than 6 months)
                    {"Date": (base_date - timedelta(days=210)).strftime("%Y-%m-%d %H:%M:%S"), "Likes": "50"},
                ]
            }
        },
        "Direct Messages": {
            "Chat History": {
                "ChatHistory": {
                    "chat1": [
                        # Recent messages (past 6 months)
                        {"From": "testuser", "Date": (base_date - timedelta(days=5)).strftime("%Y-%m-%d %H:%M:%S")},
                        {"From": "friend", "Date": (base_date - timedelta(days=10)).strftime("%Y-%m-%d %H:%M:%S")},
                        {"From": "testuser", "Date": (base_date - timedelta(days=15)).strftime("%Y-%m-%d %H:%M:%S")},
                        # Old messages (more than 6 months)
                        {"From": "testuser", "Date": (base_date - timedelta(days=230)).strftime("%Y-%m-%d %H:%M:%S")},
                        {"From": "friend", "Date": (base_date - timedelta(days=240)).strftime("%Y-%m-%d %H:%M:%S")},
                    ]
                }
            }
        }
    }

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.writestr("user_data.json", json.dumps(test_data))
    zip_buffer.seek(0)
    return zip_buffer


def test_summary_includes_filtered_counts():
    """Test that summary table includes filtered counts for past 6 months"""
    test_zip = create_test_zip_with_mixed_dates()
    meta_data = []
    results = extract_tiktok_data(test_zip, "en", meta_data)

    summary = next((r for r in results if r.id == "tiktok_summary"), None)
    assert summary is not None

    df = summary.data_frame

    # Check that we have the expected rows (9 total + 7 recent)
    assert len(df) == 16, f"Expected 16 rows in summary, got {len(df)}"

    # Verify total counts
    videos_viewed_total = df[df["Description"] == "Videos viewed"]["Number"].iloc[0]
    assert videos_viewed_total == 5, f"Expected 5 total videos viewed, got {videos_viewed_total}"

    likes_given_total = df[df["Description"] == "Likes given"]["Number"].iloc[0]
    assert likes_given_total == 3, f"Expected 3 total likes given, got {likes_given_total}"

    comments_total = df[df["Description"] == "Comments published"]["Number"].iloc[0]
    assert comments_total == 3, f"Expected 3 total comments, got {comments_total}"

    videos_published_total = df[df["Description"] == "Videos published"]["Number"].iloc[0]
    assert videos_published_total == 2, f"Expected 2 total videos published, got {videos_published_total}"

    messages_sent_total = df[df["Description"] == "Messages sent"]["Number"].iloc[0]
    assert messages_sent_total == 3, f"Expected 3 total messages sent, got {messages_sent_total}"

    messages_received_total = df[df["Description"] == "Messages received"]["Number"].iloc[0]
    assert messages_received_total == 2, f"Expected 2 total messages received, got {messages_received_total}"

    # Verify filtered counts (past 6 months)
    videos_viewed_recent = df[df["Description"] == "Videos viewed (past 6 months)"]["Number"].iloc[0]
    assert videos_viewed_recent == 3, f"Expected 3 recent videos viewed, got {videos_viewed_recent}"

    likes_given_recent = df[df["Description"] == "Likes given (past 6 months)"]["Number"].iloc[0]
    assert likes_given_recent == 2, f"Expected 2 recent likes given, got {likes_given_recent}"

    comments_recent = df[df["Description"] == "Comments published (past 6 months)"]["Number"].iloc[0]
    assert comments_recent == 2, f"Expected 2 recent comments, got {comments_recent}"

    videos_published_recent = df[df["Description"] == "Video posts (past 6 months)"]["Number"].iloc[0]
    assert videos_published_recent == 1, f"Expected 1 recent video published, got {videos_published_recent}"

    messages_sent_recent = df[df["Description"] == "Messages sent (past 6 months)"]["Number"].iloc[0]
    assert messages_sent_recent == 2, f"Expected 2 recent messages sent, got {messages_sent_recent}"

    messages_received_recent = df[df["Description"] == "Messages received (past 6 months)"]["Number"].iloc[0]
    assert messages_received_recent == 1, f"Expected 1 recent message received, got {messages_received_recent}"

    # Verify sessions count exists
    sessions_recent = df[df["Description"] == "Sessions (past 6 months)"]["Number"].iloc[0]
    assert sessions_recent >= 0, f"Expected non-negative sessions count, got {sessions_recent}"


def test_summary_with_locale():
    """Test that summary filtered counts work with different locales"""
    test_zip = create_test_zip_with_mixed_dates()

    # Test with German locale
    results_de = extract_tiktok_data(test_zip, "de")
    summary_de = next((r for r in results_de if r.id == "tiktok_summary"), None)
    assert summary_de is not None

    # Check that German translations are used
    df_de = summary_de.data_frame
    assert "Angesehene Videos (letzte 6 Monate)" in df_de["Description"].values
    assert "Videobeiträge (letzte 6 Monate)" in df_de["Description"].values

    # Test with Dutch locale
    results_nl = extract_tiktok_data(test_zip, "nl")
    summary_nl = next((r for r in results_nl if r.id == "tiktok_summary"), None)
    assert summary_nl is not None

    # Check that Dutch translations are used
    df_nl = summary_nl.data_frame
    assert "Bekeken video's (afgelopen 6 maanden)" in df_nl["Description"].values
    assert "Videoposts (afgelopen 6 maanden)" in df_nl["Description"].values


def test_summary_empty_data():
    """Test that summary works with empty/minimal data"""
    test_data = {
        "Profile": {
            "Profile Information": {
                "ProfileMap": {
                    "userName": "testuser",
                }
            }
        },
    }

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.writestr("user_data.json", json.dumps(test_data))
    zip_buffer.seek(0)

    results = extract_tiktok_data(zip_buffer, "en")
    summary = next((r for r in results if r.id == "tiktok_summary"), None)
    assert summary is not None

    # Should have 16 rows with mostly zeros
    df = summary.data_frame
    assert len(df) == 16


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
