import itertools
import port.api.props as props
from port.api.commands import CommandSystemDonate, CommandUIRender

import pandas as pd
import zipfile
import json
import time


def process(sessionId):
    key = "zip-contents-example"
    meta_data = []
    meta_data.append(("debug", f"{key}: start"))

    # STEP 1: select the file
    data = None
    while True:
        meta_data.append(("debug", f"{key}: prompt file"))
        promptFile = prompt_file("application/zip, text/plain")
        fileResult = yield render_data_submission_page(
            [prompt_hello_world(), promptFile]
        )
        if fileResult.__type__ == "PayloadString":
            # Extracting the zipfile
            meta_data.append(("debug", f"{key}: extracting file"))
            extraction_result = []
            zipfile_ref = get_zipfile(fileResult.value)
            print(zipfile_ref, fileResult.value)
            files = get_files(zipfile_ref)
            fileCount = len(files)
            for index, filename in enumerate(files):
                percentage = ((index + 1) / fileCount) * 100
                promptMessage = prompt_extraction_message(
                    f"Extracting file: {filename}", percentage
                )
                yield render_data_submission_page(promptMessage)
                file_extraction_result = extract_file(zipfile_ref, filename)
                extraction_result.append(file_extraction_result)

            if len(extraction_result) >= 0:
                meta_data.append(
                    ("debug", f"{key}: extraction successful, go to consent form")
                )
                data = extraction_result
                break
            else:
                meta_data.append(
                    ("debug", f"{key}: prompt confirmation to retry file selection")
                )
                retry_result = yield render_data_submission_page(retry_confirmation())
                if retry_result.__type__ == "PayloadTrue":
                    meta_data.append(("debug", f"{key}: skip due to invalid file"))
                    continue
                else:
                    meta_data.append(("debug", f"{key}: retry prompt file"))
                    break

    # STEP 2: ask for consent
    meta_data.append(("debug", f"{key}: prompt consent"))
    for prompt in prompt_consent(data):
        result = yield prompt
        if result.__type__ == "PayloadJSON":
            meta_data.append(("debug", f"{key}: donate consent data"))
            meta_frame = pd.DataFrame(meta_data, columns=["type", "message"])
            data_submission_data = json.loads(result.value)
            data_submission_data["meta"] = meta_frame.to_json()
            yield donate(f"{sessionId}-{key}", json.dumps(data_submission_data))
        if result.__type__ == "PayloadFalse":
            value = json.dumps('{"status" : "data_submission declined"}')
            yield donate(f"{sessionId}-{key}", value)


def render_data_submission_page(body):
    header = props.PropsUIHeader(
        props.Translatable(
            {"en": "Summary information", "nl": "Samenvatting gegevens"}
        ),
        pd.DataFrame(summary_data),
        description,
    )


def extract_videos_viewed(data):
    videos = get_activity_video_browsing_list_data(data)

    df = pd.DataFrame(videos, columns=["Date", "Link"])
    date = df["Date"].map(parse_datetime)
    df["Timeslot"] = pd.Series() if date.empty else map_to_timeslot(date.dt.hour)
    df = df.reindex(columns=["Date", "Timeslot", "Link"])

    description = props.Translatable(
        {
            "en": "This table contains the videos you watched on TikTok.",
        }
    )

    return ExtractionResult(
        "tiktok_videos_viewed",
        props.Translatable({"en": "Video views", "nl": "Videos gezien"}),
        df,
        description,
    )


def extract_video_posts(data):
    video_list = get_in(data, "Video", "Videos", "VideoList")
    if video_list is None:
        return
    videos = get_date_filtered_items(video_list)
    post_stats = defaultdict(lambda: defaultdict(int))
    for date, video in videos:
        hourly_stats = post_stats[hourly_key(date)]
        hourly_stats["Videos"] += 1
        hourly_stats["Likes received"] += int(video["Likes"])

    df = pd.DataFrame(post_stats).transpose()
    if df.empty:
        df["Date"] = pd.Series()
        df["Timselot"] = pd.Series()
    else:
        df["Date"] = df.index.strftime("%Y-%m-%d")
        df["Timeslot"] = map_to_timeslot(df.index.hour)
    df = df.reset_index(drop=True)
    df = df.reindex(columns=["Date", "Timeslot", "Videos", "Likes received"])

    description = props.Translatable(
        {
            "en": "This table contains the number of videos you yourself posted and the number of likes you received. For anonymization purposes, videos are grouped by the hour in which they were posted and the exact time is removed.",
        }
    )

    return ExtractionResult(
        "tiktok_posts",
        props.Translatable({"en": "Video posts", "nl": "Video posts"}),
        df,
        description,
    )


def extract_comments_and_likes(data):
    comments = get_all_first(
        get_date_filtered_items(get_list(data, "Comment", "Comments", "CommentsList"))
    )
    comment_counts = get_count_by_date_key(comments, hourly_key)

    likes_given = get_all_first(
        get_date_filtered_items(
            get_list(data, "Activity", "Like List", "ItemFavoriteList")
        )
    )
    likes_given_counts = get_count_by_date_key(likes_given, hourly_key)
    if not likes_given_counts:
        return

    df1 = pd.DataFrame(comment_counts, columns=["Date", "Comment posts"]).set_index(
        "Date"
    )
    df2 = pd.DataFrame(likes_given_counts, columns=["Date", "Likes given"]).set_index(
        "Date"
    )

    df = pd.merge(df1, df2, left_on="Date", right_on="Date", how="outer").sort_index()
    df["Timeslot"] = map_to_timeslot(df.index.hour)
    df["Date"] = df.index.strftime("%Y-%m-%d %H:00:00")
    df = (
        df.reindex(columns=["Date", "Timeslot", "Comment posts", "Likes given"])
        .reset_index(drop=True)
        .fillna(0)
    )
    df["Comment posts"] = df["Comment posts"].astype(int)
    df["Likes given"] = df["Likes given"].astype(int)

    description = props.Translatable(
        {
            "en": "This table contains the number of likes you gave and comments you made.",
        }
    )

    return ExtractionResult(
        "tiktok_comments_and_likes",
        props.Translatable({"en": "Comments and likes", "nl": "Comments en likes"}),
        df,
        description,
    )


def extract_session_info(data):
    session_paths = [
        ("Video", "Videos", "VideoList"),
        ("Activity", "Video Browsing History", "VideoList"),
        ("Comment", "Comments", "CommentsList"),
    ]

    item_lists = [get_list(data, *path) for path in session_paths]
    dates = get_all_first(get_date_filtered_items(itertools.chain(*item_lists)))

    sessions = get_sessions(dates)
    df = pd.DataFrame(sessions, columns=["Start", "End", "Duration"])
    if df.empty:
        df["Start"] = pd.Series()
        df["Duration (in minutes)"] = pd.Series()
    else:
        df["Start"] = df["Start"].dt.strftime("%Y-%m-%d %H:%M")
        df["Duration (in minutes)"] = (df["Duration"].dt.total_seconds() / 60).round(2)
    df = df.drop("End", axis=1)
    df = df.drop("Duration", axis=1)

    description = props.Translatable(
        {
            "en": "This table contains the start date and duration of your TikTok sessions.",
        }
    )

    return ExtractionResult(
        "tiktok_session_info",
        props.Translatable({"en": "Session information", "nl": "Sessie informatie"}),
        df,
        description,
    )


def extract_direct_messages(data):
    history = get_in(data, "Direct Messages", "Chat History", "ChatHistory")
    counter = itertools.count(start=1)
    anon_ids = defaultdict(lambda: next(counter))
    # Ensure 1 is the ID of the donating user
    anon_ids[get_user_name(data)]
    table = {"Anonymous ID": [], "Sent": []}
    for item in flatten_chat_history(history):
        table["Anonymous ID"].append(anon_ids[item["From"]])
        print(item)
        table["Sent"].append(parse_datetime(item["Date"]).strftime("%Y-%m-%d %H:%M"))

    description = props.Translatable(
        {
            "en": "This table contains the times at which you sent or received direct messages. The content of the messages is not included, and user names are replaced with anonymous IDs.",
        }
    )

    return ExtractionResult(
        "tiktok_direct_messages",
        props.Translatable(
            {"en": "Direct Message Activity", "nl": "Berichten activiteit"}
        ),
        pd.DataFrame(table),
        description,
    )


## REMOVED BY REQUEST FROM CAMBRIDGE (see notion)
def extract_comment_activity(data):
    comments = get_in(data, "Comment", "Comments", "CommentsList")
    if comments is None:
        return
    timestamps = [
        parse_datetime(item["Date"]).strftime("%Y-%m-%d %H:%M") for item in comments
    ]

    return ExtractionResult(
        "tiktok_comment_activity",
        props.Translatable({"en": "Comment Activity", "nl": "Commentaar activiteit"}),
        pd.DataFrame({"Posted on": timestamps}),
        None,
    )


## REMOVED BY REQUEST FROM CAMBRIDGE (see notion)
def extract_videos_liked(data):
    favorite_videos = get_in(data, "Activity", "Favorite Videos", "FavoriteVideoList")
    if favorite_videos is None:
        return
    table = {"Liked": []}
    for item in favorite_videos:
        table["Liked"].append(parse_datetime(item["Date"]).strftime("%Y-%m-%d %H:%M"))

    return ExtractionResult(
        "tiktok_videos_liked",
        props.Translatable({"en": "Videos liked", "nl": "Gelikete videos"}),
        pd.DataFrame(table),
        None,
    )


def extract_tiktok_data(zip_file):
    extractors = [
        extract_summary_data,
        extract_video_posts,
        extract_comments_and_likes,
        extract_videos_viewed,
        extract_session_info,
        extract_direct_messages,
    ]
    for data in get_json_data_from_file(zip_file):
        return [
            table
            for table in (extractor(data) for extractor in extractors)
            if table is not None
        ]


######################
# Data donation flow #
######################

ExtractionResult = namedtuple(
    "ExtractionResult", ["id", "title", "data_frame", "description"]
)


class InvalidFileError(Exception):
    """Indicates that the file does not match expectations."""


class SkipToNextStep(Exception):
    pass


class DataDonationProcessor:
    def __init__(self, platform, mime_types, extractor, session_id):
        self.platform = platform
        self.mime_types = mime_types
        self.extractor = extractor
        self.session_id = session_id
        self.progress = 0
        self.meta_data = []

    def process(self):
        with suppress(SkipToNextStep):
            while True:
                file_result = yield from self.prompt_file()

                self.log(f"extracting file")
                try:
                    extraction_result = self.extract_data(file_result.value)
                except IOError as e:
                    self.log(f"prompt confirmation to retry file selection")
                    yield from self.prompt_retry()
                    return
                except InvalidFileError:
                    self.log(f"invalid file detected, prompting for retry")
                    if (yield from self.prompt_retry()):
                        continue
                    else:
                        return
                else:
                    if extraction_result is None:
                        try_again = yield from self.prompt_retry()
                        if try_again:
                            continue
                        else:
                            return
                    self.log(f"extraction successful, go to consent form")
                    yield from self.prompt_consent(extraction_result)
                    return

    def prompt_retry(self):
        retry_result = yield render_donation_page(
            self.platform, [retry_confirmation(self.platform)]
        )
        return retry_result.__type__ == "PayloadTrue"

    def prompt_file(self):
        description = props.Translatable(
            {
                "en": f"Pick the file that you received from TikTok. The data that is required for research is extracted from your file in the next step. This may take a while, thank you for your patience.",
                "nl": f"Klik op ‘Kies bestand’ om het bestand dat u ontvangen hebt van TikTok te kiezen. Als u op 'Verder' klikt worden de gegevens die nodig zijn voor het onderzoek uit uw bestand gehaald. Dit kan soms even duren. Een moment geduld a.u.b.",
            }
        )
        prompt_file = props.PropsUIPromptFileInput(description, self.mime_types)
        file_result = yield render_donation_page(self.platform, [prompt_file])
        if file_result.__type__ != "PayloadString":
            self.log(f"skip to next step")
            raise SkipToNextStep()
        return file_result

    def log(self, message):
        self.meta_data.append(("debug", f"{self.platform}: {message}"))

    def extract_data(self, file):
        return self.extractor(file)

    def prompt_consent(self, data):
        log_title = props.Translatable({"en": "Log messages", "nl": "Log berichten"})

        tables = [
            props.PropsUIPromptConsentFormTable(
                table.id,
                table.title,
                table.description,
                table.data_frame,
            )
            for table in data
        ]
        # meta_frame = pd.DataFrame(self.meta_data, columns=["type", "message"])

        self.log(f"prompt consent")
        description = props.Translatable(
            {
                "en": """Decide whether you would like to donate the data below. Carefully check the data and adjust as required. Your donation will contibute to the reasearch project that was explained at the start of the project. Thank you in advance.
                    If you DO NOT want to donate information from some of the tables below, you can select a table row and delete it or click the checkbox on the left table top to select all information in the table and click 'Delete'.""",
            }
        )
        consent_result = yield render_donation_page(
            self.platform,
            tables
            + [
                props.PropsUIDataSubmissionButtons(
                    donate_question=props.Translatable(
                        {
                            "en": "Would you like to donate this data?",
                            "de": "Möchten Sie diese Daten spenden?",
                            "it": "Vuoi donare questi dati?",
                            "nl": "Wilt u deze gegevens doneren?",
                        }
                    ),
                    donate_button=props.Translatable(
                        {"en": "Donate", "de": "Spenden", "it": "Dona", "nl": "Doneren"}
                    ),
                ),
            ],
        )

        if consent_result.__type__ == "PayloadJSON":
            self.log(f"donate consent data")
            yield donate(f"{self.session_id}-{self.platform}", consent_result.value)


class DataDonation:
    def __init__(self, platform, mime_types, extractor):
        self.platform = platform
        self.mime_types = mime_types
        self.extractor = extractor

    def __call__(self, session_id):
        processor = DataDonationProcessor(
            self.platform, self.mime_types, self.extractor, session_id
        )
        yield from processor.process()


tik_tok_data_donation = DataDonation(
    "TikTok", "application/zip, text/plain, application/json", extract_tiktok_data
)


def process(session_id):
    yield donate(f"{session_id}-tracking", '[{ "message": "user entered script" }]')
    yield from tik_tok_data_donation(session_id)


def render_donation_page(platform, body):
    header = props.PropsUIHeader(props.Translatable({"en": platform, "nl": platform}))
    page = props.PropsUIPageDataSubmission(platform, header, body)
    return CommandUIRender(page)


def retry_confirmation(platform):
    text = props.Translatable(
        {
            "en": "Unfortunately, we cannot process your data. Please make sure that you selected JSON as a file format when downloading your data from TikTok.",
            "nl": "Helaas kunnen we uw gegevens niet verwerken. Zorg ervoor dat u JSON heeft geselecteerd als bestandsformaat bij het downloaden van uw gegevens van TikTok.",
        }
    )
    ok = props.Translatable({"en": "Try again", "nl": "Probeer opnieuw"})
    cancel = props.Translatable({"en": "Continue", "nl": "Verder"})
    return props.PropsUIPromptConfirm(text, ok, cancel)


def donate(key, json_string):
    return CommandSystemDonate(key, json_string)


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        print(extract_tiktok_data(sys.argv[1]))
    else:
        print("please provide a zip file as argument")
