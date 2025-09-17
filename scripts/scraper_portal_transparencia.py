import requests
import pandas as pd
from bs4 import BeautifulSoup
import time
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def setup_driver():
    """Configura o driver do Chrome"""
    chrome_options = Options()
    chrome_options.add_argument('--headless')  # Executar em modo headless
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--window-size=1920,1080')
    
    driver = webdriver.Chrome(options=chrome_options)
    return driver

def extract_table_data(driver):
    """Extrai dados da tabela atual"""
    try:
        # Aguardar a tabela carregar
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "table"))
        )
        
        # Encontrar a tabela
        table = driver.find_element(By.TAG_NAME, "table")
        
        # Extrair cabeçalhos
        headers = []
        header_elements = table.find_elements(By.CSS_SELECTOR, "thead th")
        for header in header_elements:
            headers.append(header.text.strip())
        
        # Extrair dados das linhas
        rows_data = []
        row_elements = table.find_elements(By.CSS_SELECTOR, "tbody tr")
        
        for row in row_elements:
            cells = row.find_elements(By.TAG_NAME, "td")
            row_data = []
            for cell in cells:
                row_data.append(cell.text.strip())
            if row_data:  # Só adicionar se a linha não estiver vazia
                rows_data.append(row_data)
        
        return headers, rows_data
    
    except Exception as e:
        logger.error(f"Erro ao extrair dados da tabela: {e}")
        return [], []

def scrape_portal_transparencia():
    """Função principal para fazer a raspagem"""
    url = "https://portaldatransparencia.gov.br/beneficios/pe-de-meia?de=01/01/2025&ate=31/01/2025&tipoBeneficio=10&ordenarPor=mesReferencia&direcao=asc"
    
    driver = setup_driver()
    all_data = []
    headers = []
    
    try:
        logger.info("Acessando o Portal da Transparência...")
        driver.get(url)
        
        # Aguardar a página carregar
        time.sleep(5)
        
        # Tentar aceitar cookies se aparecer
        try:
            cookie_button = WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Aceitar') or contains(text(), 'OK')]"))
            )
            cookie_button.click()
            time.sleep(2)
        except:
            logger.info("Popup de cookies não encontrado ou já aceito")
        
        # Aguardar a tabela aparecer
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.TAG_NAME, "table"))
        )
        
        # Extrair dados da primeira página
        logger.info("Extraindo dados da primeira página...")
        headers, page_data = extract_table_data(driver)
        
        if not headers:
            logger.error("Não foi possível extrair os cabeçalhos da tabela")
            return
        
        all_data.extend(page_data)
        logger.info(f"Página 1: {len(page_data)} registros coletados")
        
        # Navegar pelas páginas seguintes
        page_num = 2
        max_pages = 2000  # Limite de segurança (20000 registros / 10 por página)
        
        while page_num <= max_pages:
            try:
                # Procurar botão "próxima página"
                next_button = driver.find_element(By.CSS_SELECTOR, "#lista_next button")
                
                # Verificar se o botão está desabilitado (última página)
                parent_li = next_button.find_element(By.XPATH, "..")
                if "disabled" in parent_li.get_attribute("class"):
                    logger.info("Última página atingida")
                    break
                
                # Clicar no botão próxima página
                driver.execute_script("arguments[0].click();", next_button)
                time.sleep(3)  # Aguardar carregar
                
                # Extrair dados da página atual
                _, page_data = extract_table_data(driver)
                
                if not page_data:
                    logger.info("Nenhum dado encontrado na página, parando...")
                    break
                
                all_data.extend(page_data)
                logger.info(f"Página {page_num}: {len(page_data)} registros coletados. Total: {len(all_data)}")
                
                # Verificar se atingiu o limite de 20.000 registros
                if len(all_data) >= 20000:
                    logger.info("Limite de 20.000 registros atingido")
                    all_data = all_data[:20000]  # Manter apenas os primeiros 20.000
                    break
                
                page_num += 1
                
            except NoSuchElementException:
                logger.info("Botão de próxima página não encontrado, parando...")
                break
            except Exception as e:
                logger.error(f"Erro ao navegar para a próxima página: {e}")
                break
        
        # Criar DataFrame e salvar em CSV
        if all_data:
            logger.info(f"Criando DataFrame com {len(all_data)} registros...")
            
            # Ajustar dados para ter o mesmo número de colunas que os cabeçalhos
            cleaned_data = []
            for row in all_data:
                # Garantir que cada linha tenha o mesmo número de colunas
                if len(row) == len(headers):
                    cleaned_data.append(row)
                elif len(row) < len(headers):
                    # Preencher colunas faltantes com string vazia
                    row.extend([''] * (len(headers) - len(row)))
                    cleaned_data.append(row)
                else:
                    # Truncar se houver colunas extras
                    cleaned_data.append(row[:len(headers)])
            
            df = pd.DataFrame(cleaned_data, columns=headers)
            
            # Salvar em CSV
            output_file = "/home/ubuntu/dados_portal_transparencia.csv"
            df.to_csv(output_file, index=False, encoding='utf-8-sig')
            
            logger.info(f"Dados salvos em: {output_file}")
            logger.info(f"Total de registros: {len(df)}")
            logger.info(f"Colunas: {list(df.columns)}")
            
            # Mostrar algumas estatísticas
            print("\n=== RESUMO DA COLETA ===")
            print(f"Total de registros coletados: {len(df)}")
            print(f"Colunas disponíveis: {list(df.columns)}")
            print(f"Arquivo salvo em: {output_file}")
            
            # Mostrar primeiras linhas como exemplo
            print("\n=== PRIMEIRAS 5 LINHAS ===")
            print(df.head().to_string())
            
        else:
            logger.error("Nenhum dado foi coletado")
    
    except Exception as e:
        logger.error(f"Erro geral durante a raspagem: {e}")
    
    finally:
        driver.quit()

if __name__ == "__main__":
    scrape_portal_transparencia()
