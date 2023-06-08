from re import search
from requests import get
from bs4 import BeautifulSoup
import logging

INEP_BASEURL = "https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/"


def scrapy_files_from_inep(datasource: str = "enem", **kwargs):
    response = get(f"{INEP_BASEURL}{datasource}")

    if response.status_code != 200:
        raise ("Request failed")

    soup = BeautifulSoup(response.text, "html.parser")
    page_links = soup.find_all('a')

    files_to_download = []
    for url in page_links:
        # Check if link is a file with .zip or .csv extension4
        file_download_link = search(r"https.*.zip|https.*.csv", str(url.get('href', [])))
        if file_download_link:
            # Get the correspondent name of the file displayed on the page
            display_name = search(r">.*<\/a>", str(url)).group()
            files_to_download.append(
                {
                    "url": file_download_link.group(),
                    "display_name": display_name[1:-4],
                    "datasource": datasource
                }
            )

    logging.info(f"Found {len(files_to_download)} files to download from {datasource}.")
    logging.info(f"Files to download: {files_to_download}")

    return files_to_download
