import requests
from bs4 import BeautifulSoup
from pathlib import Path
from urllib.parse import urljoin


path_out = Path("data/censo_escolar")
path_out.mkdir(parents=True, exist_ok=True)


def parse_page(url):
    response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
    
    if response.status_code != 200:
        print(f"Erro ao acessar a página: {response.status_code}")
        return
    soup = BeautifulSoup(response.text, 'html.parser')

    links = [tag['href'] for tag in soup.find_all("a", class_="external-link") if tag.get("href")]
    
    if not links:
        print("Nenhum link encontrado")
        return
   
    for data in links:
        full_url = urljoin(url, data)
        print(f"Baixando links: {full_url}")

        try:
            r = requests.get(full_url, stream=True)
            name_file = full_url.split("/")[-1]
            r.raise_for_status()
            with open(path_out/name_file, "wb") as arq:
                for chunk in r.iter_content(chunk_size=8192):
                    arq.write(chunk)
            print(f"Arquivo salvo: {name_file}")
        except requests.exceptions.RequestException as e:
            print(f'Erro ao baixar {full_url}: {e}')  

if __name__ == '__main__':
    url = 'https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/censo-escolar'
    parse_page(url)
