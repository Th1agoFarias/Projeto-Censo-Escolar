import requests
from bs4 import BeautifulSoup
from pathlib import Path
from urllib.parse import urljoin



class DataCollector:

    def __init__(self, base_url, output_dir):
        self. base_url = base_url
        self.output_dir = Path(output_dir)
        self.headers = {"User-Agent": "Mozilla/5.0"}

    def fecth_page_content(self):
        response = requests.get(self.base_url, headers=self.headers)
        response.raise_for_status()

        return response.text

    def extract_links(self, soup):
        links =[]
        for tag in soup.find_all("a", class_="external-link"):
            href = tag.get("href")
            absolute_url = urljoin(self.base_url, href)
            links.append(absolute_url)
        
        if not links:
            print("Nenhum link econtrado")
        return links

    def dowload_file(self,url):
        try:
            with requests.get(url, stream=True) as response:
                response.raise_for_status()
                file_name = url.split("/")[-1]
                file_path = self.output_dir / file_name
                with open(file_path, "wb") as file:
                    for chunk in response.iter_content(chunk_size=8192):
                        file.write(chunk)
                print(f"Arquivo baixado: {file_name}")
        except requests.RequestException as e:
            print(f"Erro ao baixar o arquivo {url}: {e}")

    def ensure_directory_exists(self):
        """Garante que o diretório de saída exista."""
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def parse_page(self):
        page_content = self.fecth_page_content()
        soup = BeautifulSoup(page_content, 'html.parser')
        links = self.extract_links(soup)

        self.ensure_directory_exists()

        for link in links:
            print(f'Baixando link: {link}')
            self.dowload_file(link)


if __name__ == '__main__':
    # Configurações
    BASE_URL = 'https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/censo-escolar'
    OUTPUT_DIR = "data/censo_escolar"

    # Instanciando e executando o parser
    collector = DataCollector(BASE_URL, OUTPUT_DIR)
    collector.parse_page()