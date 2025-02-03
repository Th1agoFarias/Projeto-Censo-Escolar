import requests
from bs4 import BeautifulSoup
from pathlib import Path
from urllib.parse import urljoin

BASE_URL = 'https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/censo-escolar'
DATA_DIR = Path("data/censo_escolar")
HEADERS = {"User-Agent": "Mozilla/5.0"}


def fecth_page_content(url):
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()

    return response.text

def extract_links(soup):
    links =[]
    for tag in soup.find_all("a", class_="external-link"):
        href = tag.get("href")
        if href:
            links.append(href)
    
    if not links:
        print("Nenhum link econtrado")
    return links

def dowload_file(url, output_dir):
    try:
        with requests.get(url, stream=True) as response:
            response.raise_for_status()
            file_name = url.split("/")[-1]
            file_path = output_dir / file_name
            with open(file_path, "wb") as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)
            print(f"Arquivo baixado: {file_name}")
    except requests.RequestException as e:
        print(f"Erro ao baixar o arquivo {url}: {e}")

def ensure_directory_exists(directory):
    """Garante que o diretório de saída exista."""
    directory.mkdir(parents=True, exist_ok=True)

def parse_page(url):
    page_content = fecth_page_content(url)
    soup = BeautifulSoup(page_content, 'html.parser')
    links = extract_links(soup)

    ensure_directory_exists(DATA_DIR)

    for link in links:
        print(f'Baixando link: {link}')
        dowload_file(link, DATA_DIR)

if __name__ == '__main__':
    parse_page(BASE_URL)