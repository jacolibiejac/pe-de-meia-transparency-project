import pandas as pd
import time
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def extract_data_from_current_page():
    """Extrai dados usando JavaScript no navegador já aberto"""
    
    # Conectar ao navegador existente
    chrome_options = Options()
    chrome_options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
    
    try:
        driver = webdriver.Chrome(options=chrome_options)
        
        # Navegar para a página se necessário
        current_url = driver.current_url
        if "portaldatransparencia.gov.br" not in current_url:
            driver.get("https://portaldatransparencia.gov.br/beneficios/pe-de-meia?de=01/01/2025&ate=31/01/2025&tipoBeneficio=10&ordenarPor=mesReferencia&direcao=asc")
            time.sleep(5)
        
        # Script JavaScript para extrair todos os dados
        js_script = """
        function extractAllData() {
            let allData = [];
            let headers = [];
            
            // Extrair cabeçalhos
            const table = document.querySelector('table');
            if (!table) return { error: 'Tabela não encontrada' };
            
            const headerElements = table.querySelectorAll('thead th');
            headers = Array.from(headerElements).map(th => th.textContent.trim());
            
            // Função para extrair dados da página atual
            function extractCurrentPageData() {
                const rows = Array.from(table.querySelectorAll('tbody tr'));
                return rows.map(row => {
                    const cells = Array.from(row.querySelectorAll('td'));
                    return cells.map(cell => cell.textContent.trim());
                });
            }
            
            // Extrair dados da primeira página
            let currentPageData = extractCurrentPageData();
            allData.push(...currentPageData);
            
            return {
                headers: headers,
                data: allData,
                totalRecords: allData.length
            };
        }
        
        return extractAllData();
        """
        
        # Executar o script
        result = driver.execute_script(js_script)
        
        if 'error' in result:
            logger.error(f"Erro no JavaScript: {result['error']}")
            return None
        
        logger.info(f"Dados extraídos: {result['totalRecords']} registros")
        return result
        
    except Exception as e:
        logger.error(f"Erro ao conectar com o navegador: {e}")
        return None

def scrape_with_pagination():
    """Versão que navega pelas páginas"""
    chrome_options = Options()
    chrome_options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
    
    try:
        driver = webdriver.Chrome(options=chrome_options)
        
        # Navegar para a página se necessário
        current_url = driver.current_url
        if "portaldatransparencia.gov.br" not in current_url:
            driver.get("https://portaldatransparencia.gov.br/beneficios/pe-de-meia?de=01/01/2025&ate=31/01/2025&tipoBeneficio=10&ordenarPor=mesReferencia&direcao=asc")
            time.sleep(5)
        
        all_data = []
        headers = []
        page_num = 1
        
        while len(all_data) < 20000:  # Limite de 20.000 registros
            logger.info(f"Processando página {page_num}...")
            
            # Extrair dados da página atual
            js_extract = """
            const table = document.querySelector('table');
            if (!table) return { error: 'Tabela não encontrada' };
            
            // Extrair cabeçalhos (só na primeira vez)
            const headers = Array.from(table.querySelectorAll('thead th')).map(th => th.textContent.trim());
            
            // Extrair dados das linhas
            const rows = Array.from(table.querySelectorAll('tbody tr'));
            const data = rows.map(row => {
                const cells = Array.from(row.querySelectorAll('td'));
                return cells.map(cell => cell.textContent.trim());
            });
            
            return { headers: headers, data: data };
            """
            
            result = driver.execute_script(js_extract)
            
            if 'error' in result:
                logger.error(f"Erro ao extrair dados: {result['error']}")
                break
            
            if not headers:
                headers = result['headers']
            
            page_data = result['data']
            if not page_data:
                logger.info("Nenhum dado encontrado, parando...")
                break
            
            all_data.extend(page_data)
            logger.info(f"Página {page_num}: {len(page_data)} registros. Total: {len(all_data)}")
            
            # Verificar se há próxima página
            try:
                next_button_script = """
                const nextButton = document.querySelector('#lista_next button');
                const parentLi = nextButton.parentElement;
                
                if (parentLi.classList.contains('disabled')) {
                    return { hasNext: false };
                } else {
                    nextButton.click();
                    return { hasNext: true };
                }
                """
                
                nav_result = driver.execute_script(next_button_script)
                
                if not nav_result['hasNext']:
                    logger.info("Última página atingida")
                    break
                
                # Aguardar a próxima página carregar
                time.sleep(3)
                page_num += 1
                
            except Exception as e:
                logger.error(f"Erro ao navegar para próxima página: {e}")
                break
        
        # Limitar a 20.000 registros
        if len(all_data) > 20000:
            all_data = all_data[:20000]
            logger.info("Limitado a 20.000 registros")
        
        return {'headers': headers, 'data': all_data, 'totalRecords': len(all_data)}
        
    except Exception as e:
        logger.error(f"Erro durante a raspagem: {e}")
        return None

def main():
    logger.info("Iniciando raspagem do Portal da Transparência...")
    
    # Tentar raspagem com paginação
    result = scrape_with_pagination()
    
    if not result:
        logger.error("Falha na raspagem")
        return
    
    # Criar DataFrame
    headers = result['headers']
    data = result['data']
    
    # Limpar dados (garantir mesmo número de colunas)
    cleaned_data = []
    for row in data:
        if len(row) == len(headers):
            cleaned_data.append(row)
        elif len(row) < len(headers):
            row.extend([''] * (len(headers) - len(row)))
            cleaned_data.append(row)
        else:
            cleaned_data.append(row[:len(headers)])
    
    df = pd.DataFrame(cleaned_data, columns=headers)
    
    # Salvar em CSV
    output_file = "/home/ubuntu/dados_portal_transparencia.csv"
    df.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    logger.info(f"Dados salvos em: {output_file}")
    logger.info(f"Total de registros: {len(df)}")
    logger.info(f"Colunas: {list(df.columns)}")
    
    # Mostrar resumo
    print("\n=== RESUMO DA COLETA ===")
    print(f"Total de registros coletados: {len(df)}")
    print(f"Colunas disponíveis: {list(df.columns)}")
    print(f"Arquivo salvo em: {output_file}")
    
    # Mostrar primeiras linhas
    print("\n=== PRIMEIRAS 5 LINHAS ===")
    print(df.head().to_string())
    
    # Mostrar estatísticas por UF
    if 'UF' in df.columns:
        print("\n=== DISTRIBUIÇÃO POR UF ===")
        print(df['UF'].value_counts().head(10))

if __name__ == "__main__":
    main()
