import os
import urllib
from functools import cached_property

from airflow.exceptions import AirflowException
from airflow.notifications.basenotifier import BaseNotifier
from airflow.providers.http.hooks.http import HttpHook

AIRFLOW_URL = os.environ.get("AIRFLOW__WEBSERVER__BASE_URL")


class MSTeamsHook(HttpHook):
    """
    This hook allows you to post messages to MS Teams using the Incoming Webhook connector.

    Takes both MS Teams webhook token directly and connection that has MS Teams webhook token.
    If both supplied, the webhook token will be appended to the host in the connection.

    :param http_conn_id: connection that has MS Teams webhook URL
    :type http_conn_id: str
    :param webhook_token: MS Teams webhook token
    :type webhook_token: str
    :param message: The message you want to send on MS Teams
    :type message: str
    :param subtitle: The subtitle of the message to send
    :type subtitle: str
    :param button_text: The text of the action button
    :type button_text: str
    :param button_url: The URL for the action button click
    :type button_url : str
    :param theme_color: Hex code of the card theme, without the #
    :type message: str
    :param proxy: Proxy to use when making the webhook request
    :type proxy: str

    """

    default_conn_name = "ms_teams"
    conn_type = "http"
    hook_name = "MS Teams"

    def __init__(
        self,
        http_conn_id=default_conn_name,
        webhook_token=None,
        message="",
        subtitle="",
        button_text="",
        button_url="",
        theme_color="00FF00",
        proxy=None,
        *args,
        **kwargs,
    ):
        super(MSTeamsHook, self).__init__(*args, **kwargs)
        self.http_conn_id = http_conn_id
        self.webhook_token = self.get_token(webhook_token, http_conn_id)
        self.message = message
        self.subtitle = subtitle
        self.button_text = button_text
        self.button_url = button_url
        self.theme_color = theme_color
        self.proxy = proxy

    def get_proxy(self, http_conn_id):
        conn = self.get_connection(http_conn_id)
        extra = conn.extra_dejson
        return extra.get("proxy", "")

    def get_token(self, token, http_conn_id):
        """
        Given either a manually set token or a conn_id, return the webhook_token to use
        :param token: The manually provided token
        :param conn_id: The conn_id provided
        :return: webhook_token (str) to use
        """
        if token:
            return token
        elif http_conn_id:
            conn = self.get_connection(http_conn_id)
            extra = conn.extra_dejson
            return extra.get("webhook_token", "")
        else:
            raise AirflowException(
                "Cannot get URL: No valid MS Teams " "webhook URL nor conn_id supplied"
            )

    def build_message(self):
        cardjson = """
                {{
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "themeColor": "{3}",
            "summary": "{0}",
            "sections": [{{
                "activityTitle": "{1}",
                "activitySubtitle": "{2}",
                "markdown": true,
                "potentialAction": [
                    {{
                        "@type": "OpenUri",
                        "name": "{4}",
                        "targets": [
                            {{ "os": "default", "uri": "{5}" }}
                        ]
                    }}
                ]
            }}]
            }}
                """
        return cardjson.format(
            self.message,
            self.message,
            self.subtitle,
            self.theme_color,
            self.button_text,
            self.button_url,
        )

    def execute(self):
        """
        Remote Popen (actually execute the webhook call)

        :param cmd: command to remotely execute
        :param kwargs: extra arguments to Popen (see subprocess.Popen)
        """
        proxies = {}
        proxy_url = self.get_proxy(self.http_conn_id)
        if len(proxy_url) > 5:
            proxies = {"https": proxy_url}
        self.run(
            endpoint=self.webhook_token,
            data=self.build_message(),
            headers={"Content-type": "application/json"},
            extra_options={"proxies": proxies},
        )


class MSTeamsNotifier(BaseNotifier):
    template_fields = ("message",)

    def __init__(
        self,
        message: str = None,
        connection_id: str = MSTeamsHook.default_conn_name,
        title: str = "Airflow DAG Status",
        subtitle: str = "",
        button_text: str = "Logs",
        button_url: str = "",
        theme_color: str = "AAAAAA",
        proxy: str = None,
    ):
        super().__init__()
        self.http_conn_id = connection_id
        self.message = message
        self.title = title
        self.subtitle = subtitle
        self.button_text = button_text
        self.button_url = button_url
        self.theme_color = theme_color
        self.proxy = proxy

    @cached_property
    def hook(self) -> MSTeamsHook:
        return MSTeamsHook(
            http_conn_id=self.http_conn_id,
        )

    def notify(self, context):
        dag_id = context["dag_run"].dag_id
        task_id = context["task_instance"].task_id
        context["task_instance"].xcom_push(key=dag_id, value=True)
        timestamp = context["ts"]
        urlencoded_timestamp = urllib.parse.quote(timestamp)
        logs_url = "{}/log?dag_id={}&task_id={}&execution_date={}".format(
            AIRFLOW_URL, dag_id, task_id, urlencoded_timestamp
        )
        self.hook.message = self.message
        self.hook.button_text = self.button_text
        self.hook.theme_color = self.theme_color
        self.hook.button_url = logs_url
        self.hook.execute()
