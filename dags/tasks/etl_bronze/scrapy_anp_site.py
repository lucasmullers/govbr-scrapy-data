from re import search
from requests import get
from bs4 import BeautifulSoup
import logging

ANP_BASEURL = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis"


def scrapy_files_from_anp(**kwargs):
    response = get(f"{ANP_BASEURL}")

    if response.status_code != 200:
        raise ("Request failed")

    soup = BeautifulSoup(response.text, "html.parser")
    page_links = soup.find_all('a')

    files_to_download = []
    for url in page_links:
        # Check if link is a file with .zip or .csv extension4
        file_download_link = search(r"https.*/shpc/dsas/ca/.*.zip|https.*/shpc/dsas/ca/.*.csv", str(url.get('href', [])))
        if file_download_link:
            # Get the correspondent name of the file displayed on the page
            display_name = search(r">.*<\/a>", str(url)).group()
            files_to_download.append(
                {
                    "url": file_download_link.group(),
                    "display_name": display_name[1:-4],
                }
            )

    logging.info(f"Found {len(files_to_download)}")
    logging.info(f"Files to download: {files_to_download}")

    return files_to_download
