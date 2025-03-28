import itertools
import port.api.props as props
from port.api.commands import CommandSystemDonate, CommandUIRender

import pandas as pd
import zipfile
import json
import datetime
from collections import defaultdict, namedtuple
from collections.abc import MutableMapping
from contextlib import suppress


class CaseInsensitiveDict(MutableMapping):
    """
    A case-insensitive dictionary for handling TikTok data.
    Keys are stored in lowercase but original casing is preserved.
    """

    def __init__(self, data=None, **kwargs):
        self._store = {}
        self._key_map = {}  # Maps normalized keys to original casing
        if data is None:
            data = {}
        self.update(dict(data, **kwargs))

    def _normalize_key(self, key):
        """Normalize key by removing spaces and converting to lowercase."""
        return key.lower() if isinstance(key, str) else key

    def _convert_value(self, value):
        """Convert a value to use case-insensitive dictionaries for nested structures."""
        if isinstance(value, dict) and not isinstance(value, CaseInsensitiveDict):
            return CaseInsensitiveDict(value)
        elif isinstance(value, list):
            return [self._convert_value(item) for item in value]
        return value

    def __setitem__(self, key, value):
        # Convert value to use case-insensitive dictionaries
        value = self._convert_value(value)

        # Normalize key
        lower_key = self._normalize_key(key)
        self._store[lower_key] = value
        self._key_map[lower_key] = key

    def __getitem__(self, key):
        return self._store[self._normalize_key(key)]

    def __delitem__(self, key):
        lower_key = self._normalize_key(key)
        del self._store[lower_key]
        del self._key_map[lower_key]

    def __iter__(self):
        return iter(self._key_map.values())

    def __len__(self):
        return len(self._store)

    def __repr__(self):
        return repr(dict(self.items()))

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def update(self, other=None, **kwargs):
        if other is not None:
            for k, v in other.items():
                self[k] = v
        if kwargs:
            for k, v in kwargs.items():
                self[k] = v


##########################
# TikTok file processing #
##########################

filter_start = datetime.datetime.now() - datetime.timedelta(weeks=4 * 6)

datetime_format = "%Y-%m-%d %H:%M:%S"


def parse_datetime(value):
    return datetime.datetime.strptime(value, datetime_format)


def get_in(data_dict, *key_path):
    for k in key_path:
        data_dict = data_dict.get(k, None)
        if data_dict is None:
            return None
    return data_dict


def get_list(data_dict, *key_path):
    result = get_in(data_dict, *key_path)
    if result is None:
        return []
    return result


def get_dict(data_dict, *key_path):
    result = get_in(data_dict, *key_path)
    if result is None:
        return {}
    return result


def get_string(data_dict, *key_path):
    result = get_in(data_dict, *key_path)
    if result is None:
        return ""
    return result


def cast_number(data_dict, *key_path):
    value = get_in(data_dict, *key_path)
    if value is None or value == "None":
        return 0
    return value


def get_activity_video_browsing_list_data(data):
    return get_list(
        data, "Activity", "Video Browsing History", "VideoList"
    ) or get_list(data, "Your Activity", "Watch History", "VideoList")


def get_comment_list_data(data):
    return get_in(data, "Comment", "Comments", "CommentsList")


def get_date_filtered_items(items):
    for item in items:
        timestamp = parse_datetime(item["Date"])
        # TODO: remove this once the script is working
        # if timestamp < filter_start:
        #     continue
        yield (timestamp, item)


def get_count_by_date_key(timestamps, key_func):
    """Returns a dict of the form (key, count)

    The key is determined by the key_func, which takes a datetime object and
    returns an object suitable for sorting and usage as a dictionary key.

    The returned list is sorted by key.
    """
    item_count = defaultdict(int)
    for timestamp in timestamps:
        item_count[key_func(timestamp)] += 1
    return sorted(item_count.items())


def get_all_first(items):
    return (i[0] for i in items)


def hourly_key(date):
    return date.replace(minute=0, second=0, microsecond=0)


def daily_key(date):
    return date.date()


def get_sessions(timestamps):
    """Returns a list of tuples of the form (start, end, duration)

    The start and end are datetime objects, and the duration is a timedelta
    object.
    """
    timestamps = list(sorted(timestamps))
    if len(timestamps) == 0:
        return []
    if len(timestamps) == 1:
        return [(timestamps[0], timestamps[0], datetime.timedelta(0))]

    sessions = []
    start = timestamps[0]
    end = timestamps[0]
    for prev, cur in zip(timestamps, timestamps[1:]):
        if cur - prev > datetime.timedelta(minutes=5):
            sessions.append((start, end, end - start))
            start = cur
        end = cur
    sessions.append((start, end, end - start))
    return sessions


def load_tiktok_data(json_file):
    data = json.load(json_file, object_hook=CaseInsensitiveDict)
    if not get_user_name(data):
        raise IOError("Unsupported file type")
    return data


def get_json_data_from_zip(zip_file):
    with zipfile.ZipFile(zip_file, "r") as zip:
        for name in zip.namelist():
            if not name.endswith(".json"):
                continue
            with zip.open(name) as json_file:
                with suppress(IOError, json.JSONDecodeError):
                    return [load_tiktok_data(json_file)]
    return []


def get_json_data_from_file(file_):
    # TikTok exports can be a single JSON file or a zipped JSON file
    try:
        if hasattr(file_, "read"):  # If it's a file-like object
            try:
                return [load_tiktok_data(file_)]
            except (json.decoder.JSONDecodeError, UnicodeDecodeError):
                file_.seek(0)  # Reset file pointer
                return get_json_data_from_zip(file_)
        else:  # If it's a file path
            with open(file_) as f:
                try:
                    return [load_tiktok_data(f)]
                except (json.decoder.JSONDecodeError, UnicodeDecodeError) as e:
                    return get_json_data_from_zip(file_)
    except (IOError, json.JSONDecodeError):
        return []


def count_items(data, *key_path):
    items = get_list(data, *key_path)
    return len(items)


def filtered_count(data, *key_path):
    items = get_list(data, *key_path)
    filtered_items = get_date_filtered_items(items)
    return len(list(filtered_items))


def get_user_name(data):
    username = get_in(data, "Profile", "Profile Information", "ProfileMap", "userName")
    if username is not None:
        return username
    return get_in(data, "Profile", "Profile Info", "ProfileMap", "userName")


def get_chat_history(data):
    return get_dict(data, "Direct Messages", "Chat History", "ChatHistory") or get_dict(
        data, "Direct Message", "Direct Messages", "ChatHistory"
    )


def flatten_chat_history(history):
    if history is None:
        return []
    return itertools.chain(*history.values())


def filter_by_key(items, key, value):
    return filter(lambda item: item[key] == value, items)


def exclude_by_key(items, key, value):
    """
    Return a filtered list where items that match key & value are excluded.
    """
    return filter(lambda item: item[key] != value, items)


def map_to_timeslot(series):
    return series.map(lambda hour: f"{hour}-{hour+1}")


def extract_summary_data(data):
    user_name = get_user_name(data)
    chat_history = get_chat_history(data)
    flattened = flatten_chat_history(chat_history)
    direct_messages = list(flattened)
    sent_count = len(
        list(filter(lambda item: item["From"] == user_name, direct_messages))
    )
    received_count = len(
        list(
            filter(
                lambda item: item["From"] != user_name,
                direct_messages,
            )
        )
    )

    summary_data = {
        "Description": [
            "Followers",
            "Following",
            "Likes received",
            "Videos posted",
            "Likes given",
            "Comments posted",
            "Messages sent",
            "Messages received",
            "Videos watched",
        ],
        "Number": [
            count_items(data, "Activity", "Follower List", "FansList")
            or count_items(data, "Your Activity", "Follower", "FansList"),
            count_items(data, "Activity", "Following List", "Following")
            or count_items(data, "Your Activity", "Following", "Following"),
            cast_number(
                data,
                "Profile",
                "Profile Information",
                "ProfileMap",
                "likesReceived",
            ),
            count_items(data, "Video", "Videos", "VideoList")
            or count_items(data, "Post", "Posts", "VideoList"),
            count_items(data, "Activity", "Like List", "ItemFavoriteList")
            or count_items(data, "Your Activity", "Like List", "ItemFavoriteList"),
            count_items(data, "Comment", "Comments", "CommentsList"),
            sent_count,
            received_count,
            count_items(data, "Activity", "Video Browsing History", "VideoList")
            or count_items(data, "Your Activity", "Watch History", "VideoList"),
        ],
    }

    description = props.Translatable(
        {
            "en": "This table contains summary information from your downloaded data. This might not match exactly with the numbers shown in your TikTok account.",
            "de": "Diese Tabelle enthält zusammengefasste Informationen aus Ihren heruntergeladenen Daten. Diese stimmen möglicherweise nicht genau mit den Zahlen in Ihrem TikTok-Konto überein.",
            "it": "Questa tabella contiene informazioni riassuntive dai tuoi dati scaricati. Questi potrebbero non corrispondere esattamente ai numeri mostrati nel tuo account TikTok.",
            "nl": "Deze tabel bevat samenvattende informatie van je gedownloade gegevens. Dit komt mogelijk niet exact overeen met de cijfers in je TikTok-account.",
        }
    )

    return ExtractionResult(
        "tiktok_summary",
        props.Translatable(
            {
                "en": "Summary information",
                "de": "Zusammenfassende Informationen",
                "it": "Informazioni riassuntive",
                "nl": "Samenvattende informatie",
            }
        ),
        pd.DataFrame(summary_data),
        description,
    )


def extract_videos_viewed(data):
    videos = get_activity_video_browsing_list_data(data)

    df = pd.DataFrame(videos, columns=["Date", "Link"])
    date = df["Date"].map(parse_datetime)
    df["Timeslot"] = (
        pd.Series(dtype="object") if date.empty else map_to_timeslot(date.dt.hour)
    )
    df = df.reindex(columns=["Date", "Timeslot", "Link"])

    description = props.Translatable(
        {
            "en": "This table contains the videos you watched on TikTok.",
            "de": "Diese Tabelle enthält die Videos, die Sie sich auf TikTok angesehen haben.",
            "it": "Questa tabella contiene i video che hai guardato su TikTok.",
            "nl": "Deze tabel bevat de video's die je op TikTok hebt bekeken.",
        }
    )

    return ExtractionResult(
        "tiktok_videos_viewed",
        props.Translatable(
            {
                "en": "Video views",
                "de": "Videoaufrufe",
                "it": "Visualizzazioni video",
                "nl": "Videoweergaven",
            }
        ),
        df,
        description,
    )


def extract_video_posts(data):
    video_list = get_in(data, "Video", "Videos", "VideoList")
    if video_list is None:
        video_list = get_in(data, "Post", "Posts", "VideoList")
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
            "de": "Diese Tabelle enthält die Anzahl der Videos, die Sie selbst gepostet haben, sowie die Anzahl der erhaltenen Likes. Zur Anonymisierung werden die Videos nach der Stunde gruppiert, in der sie gepostet wurden, und die genaue Uhrzeit wird entfernt.",
            "it": "Questa tabella contiene il numero di video che hai pubblicato e il numero di like ricevuti. Per motivi di anonimizzazione, i video sono raggruppati per ora di pubblicazione e l'orario esatto è stato rimosso.",
            "nl": "Deze tabel bevat het aantal video's dat je zelf hebt gepost en het aantal likes dat je hebt ontvangen. Voor anonimiseringsdoeleinden zijn de video's gegroepeerd per uur van plaatsing en is de exacte tijd verwijderd.",
        }
    )

    return ExtractionResult(
        "tiktok_posts",
        props.Translatable(
            {
                "en": "Video posts",
                "de": "Videobeiträge",
                "it": "Video pubblicati",
                "nl": "Videoposts",
            }
        ),
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
            or get_list(data, "Your Activity", "Like List", "ItemFavoriteList")
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
            "de": "Diese Tabelle enthält die Anzahl der vergebenen Likes und abgegebenen Kommentare.",
            "it": "Questa tabella contiene il numero di like che hai messo e i commenti che hai scritto.",
            "nl": "Deze tabel bevat het aantal likes dat je hebt gegeven en de reacties die je hebt geplaatst.",
        }
    )

    return ExtractionResult(
        "tiktok_comments_and_likes",
        props.Translatable(
            {
                "en": "Comments and likes",
                "de": "Kommentare und Likes",
                "it": "Commenti e Mi piace",
                "nl": "Reacties en likes",
            }
        ),
        df,
        description,
    )


def extract_session_info(data):
    session_paths = [
        # Old
        ("Video", "Videos", "VideoList"),
        ("Activity", "Video Browsing History", "VideoList"),
        ("Comment", "Comments", "CommentsList"),
        # New
        ("Post", "Posts", "VideoList"),
        ("Your Activity", "Favorite Videos", "FavoriteVideoList"),
        ("Your Activity", "Watch History", "VideoList"),
    ]

    item_lists = [get_list(data, *path) for path in session_paths]
    dates = get_all_first(get_date_filtered_items(itertools.chain(*item_lists)))

    sessions = get_sessions(dates)
    df = pd.DataFrame(sessions, columns=["Start", "End", "Duration"])
    if df.empty:
        df["Start"] = pd.Series(dtype="object")
        df["Duration (in minutes)"] = pd.Series(dtype="float64")
    else:
        df["Start"] = df["Start"].dt.strftime("%Y-%m-%d %H:%M")
        df["Duration (in minutes)"] = (df["Duration"].dt.total_seconds() / 60).round(2)
    df = df.drop("End", axis=1)
    df = df.drop("Duration", axis=1)

    description = props.Translatable(
        {
            "en": "This table contains the start date and duration of your TikTok sessions.",
            "de": "Diese Tabelle enthält das Startdatum und die Dauer Ihrer TikTok-Sitzungen.",
            "it": "Questa tabella contiene la data di inizio e la durata delle tue sessioni su TikTok.",
            "nl": "Deze tabel bevat de startdatum en de duur van je TikTok-sessies.",
        }
    )

    return ExtractionResult(
        "tiktok_session_info",
        props.Translatable(
            {
                "en": "Session information",
                "de": "Sitzungsinformationen",
                "it": "Informazioni sulla sessione",
                "nl": "Sessie-informatie",
            }
        ),
        df,
        description,
    )


def extract_direct_messages(data):
    history = get_in(data, "Direct Messages", "Chat History", "ChatHistory")
    if history is None:
        history = get_in(data, "Direct Message", "Direct Messages", "ChatHistory")
    counter = itertools.count(start=1)
    anon_ids = defaultdict(lambda: next(counter))
    # Ensure 1 is the ID of the donating user
    anon_ids[get_user_name(data)]
    table = {"Anonymous ID": [], "Sent": []}
    for item in flatten_chat_history(history):
        table["Anonymous ID"].append(anon_ids[item["From"]])
        table["Sent"].append(parse_datetime(item["Date"]).strftime("%Y-%m-%d %H:%M"))

    description = props.Translatable(
        {
            "en": "This table contains the times at which you sent or received direct messages. The content of the messages is not included, and user names are replaced with anonymous IDs.",
            "de": "Diese Tabelle enthält die Uhrzeiten, zu denen Sie Direktnachrichten gesendet oder empfangen haben. Der Inhalt der Nachrichten ist nicht enthalten, und Benutzernamen wurden durch anonyme IDs ersetzt.",
            "it": "Questa tabella contiene gli orari in cui hai inviato o ricevuto messaggi diretti. Il contenuto dei messaggi non è incluso e i nomi utente sono sostituiti da ID anonimi.",
            "nl": "Deze tabel bevat de tijdstippen waarop je directe berichten hebt verzonden of ontvangen. De inhoud van de berichten is niet opgenomen en gebruikersnamen zijn vervangen door anonieme ID's.",
        }
    )

    return ExtractionResult(
        "tiktok_direct_messages",
        props.Translatable(
            {
                "en": "Direct Message Activity",
                "de": "Aktivität bei Direktnachrichten",
                "it": "Attività dei messaggi diretti",
                "nl": "Activiteit met directe berichten",
            }
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
        props.Translatable(
            {
                "en": "Comment Activity",
                "de": "Kommentaraktivität",
                "it": "Attività dei commenti",
                "nl": "Reactie-activiteit",
            }
        ),
        pd.DataFrame({"Posted on": timestamps}),
        None,
    )


## REMOVED BY REQUEST FROM CAMBRIDGE (see notion)
def extract_videos_liked(data):
    favorite_videos = get_in(
        data, "Activity", "Favorite Videos", "FavoriteVideoList"
    ) or get_in(data, "Your Activity", "Favorite Videos", "FavoriteVideoList")
    if favorite_videos is None:
        return
    table = {"Liked": []}
    for item in favorite_videos:
        table["Liked"].append(parse_datetime(item["Date"]).strftime("%Y-%m-%d %H:%M"))

    return ExtractionResult(
        "tiktok_videos_liked",
        props.Translatable(
            {
                "en": "Videos liked",
                "de": "Videos mit Gefällt mir",
                "it": "Video a cui hai messo Mi piace",
                "nl": "Video's die je leuk vond",
            }
        ),
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
    data_list = get_json_data_from_file(zip_file)
    if not data_list:
        return []
    for data in data_list:
        results = []
        for extractor in extractors:
            table = extractor(data)
            if table is not None:
                results.append(table)
        return results
    return []


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
                "de": f"Wählen Sie die Datei aus, die Sie von TikTok erhalten haben. Die für die Forschung benötigten Daten werden im nächsten Schritt aus Ihrer Datei extrahiert. Dies kann eine Weile dauern – vielen Dank für Ihre Geduld.",
                "it": f"Seleziona il file che hai ricevuto da TikTok. I dati richiesti per la ricerca verranno estratti dal tuo file nel passaggio successivo. Questo potrebbe richiedere un po' di tempo, grazie per la pazienza.",
                "nl": f"Kies het bestand dat je van TikTok hebt ontvangen. De gegevens die nodig zijn voor onderzoek worden in de volgende stap uit je bestand gehaald. Dit kan even duren, bedankt voor je geduld.",
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
        log_title = props.Translatable(
            {
                "en": "Log messages",
                "de": "Protokollnachrichten",
                "it": "Messaggi di log",
                "nl": "Logberichten",
            }
        )
        tables = [
            props.PropsUIPromptConsentFormTable(
                table.id,
                table.title,
                table.description,
                table.data_frame,
            )
            for table in data
        ]

        self.log(f"prompt consent")

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
    header = props.PropsUIHeader(props.Translatable(
            {
                "en": "platform",
                "de": "Plattform",
                "it": "piattaforma",
                "nl": "platform",
            }
        )
    )
    page = props.PropsUIPageDataSubmission(platform, header, body)
    return CommandUIRender(page)


def retry_confirmation(platform):
    text = props.Translatable(
        {
            "en": "Unfortunately, we cannot process your data. Please make sure that you selected JSON as a file format when downloading your data from TikTok.",
            "de": "Leider können wir Ihre Daten nicht verarbeiten. Bitte stellen Sie sicher, dass Sie beim Herunterladen Ihrer Daten von TikTok das JSON-Format ausgewählt haben.",
            "it": "Purtroppo non possiamo elaborare i tuoi dati. Assicurati di aver selezionato il formato JSON quando hai scaricato i dati da TikTok.",
            "nl": "Helaas kunnen we je gegevens niet verwerken. Zorg ervoor dat je JSON hebt geselecteerd als bestandsformaat bij het downloaden van je gegevens van TikTok.",
        }
    )
    ok = props.Translatable(
        {
            "en": "Try again",
            "de": "Erneut versuchen",
            "it": "Riprova",
            "nl": "Probeer het opnieuw",
        }
    )
    cancel = props.Translatable(
        {
            "en": "Continue",
            "de": "Weiter",
            "it": "Continua",
            "nl": "Doorgaan",
        }
    )
    return props.PropsUIPromptConfirm(text, ok, cancel)


def donate(key, json_string):
    return CommandSystemDonate(key, json_string)


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        print(extract_tiktok_data(sys.argv[1]))
    else:
        print("please provide a zip file as argument")
