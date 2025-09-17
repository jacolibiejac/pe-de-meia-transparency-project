import pandas as pd
import time
import json
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def setup_driver():
    """Configura o driver do Chrome"""
    chrome_options = Options()
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
    
    driver = webdriver.Chrome(options=chrome_options)
    return driver

def extract_page_data(driver):
    """Extrai dados da página atual usando JavaScript"""
    js_script = """
    const table = document.querySelector('table');
    if (!table) return { error: 'Tabela não encontrada' };
    
    // Extrair cabeçalhos
    const headers = Array.from(table.querySelectorAll('thead th')).map(th => th.textContent.trim());
    
    // Extrair dados das linhas
    const rows = Array.from(table.querySelectorAll('tbody tr'));
    const data = rows.map(row => {
        const cells = Array.from(row.querySelectorAll('td'));
        return cells.map(cell => cell.textContent.trim());
    });
    
    return {
        headers: headers,
        data: data,
        rowCount: rows.length
    };
    """
    
    try:
        result = driver.execute_script(js_script)
        return result
    except Exception as e:
        logger.error(f"Erro ao extrair dados: {e}")
        return {'error': str(e)}

def navigate_to_next_page(driver):
    """Navega para a próxima página"""
    js_script = """
    const nextButton = document.querySelector('#lista_next button');
    if (!nextButton) return { error: 'Botão próxima página não encontrado' };
    
    const parentLi = nextButton.parentElement;
    if (parentLi.classList.contains('disabled')) {
        return { hasNext: false, message: 'Última página atingida' };
    }
    
    nextButton.click();
    return { hasNext: true, message: 'Navegou para próxima página' };
    """
    
    try:
        result = driver.execute_script(js_script)
        return result
    except Exception as e:
        logger.error(f"Erro ao navegar: {e}")
        return {'error': str(e)}

def scrape_all_data():
    """Função principal para raspagem"""
    url = "https://portaldatransparencia.gov.br/beneficios/pe-de-meia?de=01/01/2025&ate=31/01/2025&tipoBeneficio=10&ordenarPor=mesReferencia&direcao=asc"
    
    driver = setup_driver()
    all_data = []
    headers = []
    page_num = 1
    
    try:
        logger.info("Acessando o Portal da Transparência...")
        driver.get(url)
        
        # Aguardar a página carregar
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.TAG_NAME, "table"))
        )
        
        logger.info("Página carregada, iniciando extração...")
        
        while len(all_data) < 20000:
            logger.info(f"Processando página {page_num}...")
            
            # Aguardar um pouco para garantir que a página carregou
            time.sleep(2)
            
            # Extrair dados da página atual
            result = extract_page_data(driver)
            
            if 'error' in result:
                logger.error(f"Erro na página {page_num}: {result['error']}")
                break
            
            # Salvar cabeçalhos na primeira iteração
            if not headers and result.get('headers'):
                headers = result['headers']
                logger.info(f"Cabeçalhos identificados: {headers}")
            
            page_data = result.get('data', [])
            
            if not page_data:
                logger.info("Nenhum dado encontrado na página, parando...")
                break
            
            # Filtrar linhas vazias ou inválidas
            valid_data = [row for row in page_data if len(row) >= len(headers) and any(cell.strip() for cell in row)]
            
            all_data.extend(valid_data)
            logger.info(f"Página {page_num}: {len(valid_data)} registros válidos. Total acumulado: {len(all_data)}")
            
            # Verificar se atingiu o limite
            if len(all_data) >= 20000:
                all_data = all_data[:20000]
                logger.info("Limite de 20.000 registros atingido!")
                break
            
            # Tentar navegar para a próxima página
            nav_result = navigate_to_next_page(driver)
            
            if 'error' in nav_result:
                logger.error(f"Erro ao navegar: {nav_result['error']}")
                break
            
            if not nav_result.get('hasNext', False):
                logger.info(nav_result.get('message', 'Não há mais páginas'))
                break
            
            page_num += 1
            
            # Aguardar a próxima página carregar
            time.sleep(3)
        
        # Processar e salvar dados
        if all_data and headers:
            logger.info(f"Processando {len(all_data)} registros...")
            
            # Limpar e padronizar dados
            cleaned_data = []
            for row in all_data:
                # Garantir que cada linha tenha o mesmo número de colunas
                if len(row) >= len(headers):
                    cleaned_row = row[:len(headers)]  # Truncar se necessário
                else:
                    cleaned_row = row + [''] * (len(headers) - len(row))  # Preencher com vazios
                
                cleaned_data.append(cleaned_row)
            
            # Criar DataFrame
            df = pd.DataFrame(cleaned_data, columns=headers)
            
            # Remover linhas completamente vazias
            df = df.dropna(how='all')
            
            # Salvar em CSV
            output_file = "/home/ubuntu/dados_portal_transparencia.csv"
            df.to_csv(output_file, index=False, encoding='utf-8-sig')
            
            logger.info(f"Dados salvos em: {output_file}")
            logger.info(f"Total de registros salvos: {len(df)}")
            
            # Exibir resumo
            print("\n" + "="*50)
            print("RESUMO DA COLETA DE DADOS")
            print("="*50)
            print(f"Total de registros coletados: {len(df):,}")
            print(f"Total de páginas processadas: {page_num}")
            print(f"Arquivo salvo em: {output_file}")
            print(f"Colunas disponíveis: {list(df.columns)}")
            
            # Estatísticas por UF
            if 'UF' in df.columns:
                print(f"\nDistribuição por UF (Top 10):")
                uf_counts = df['UF'].value_counts().head(10)
                for uf, count in uf_counts.items():
                    print(f"  {uf}: {count:,} registros")
            
            # Estatísticas por valor
            if 'Valor Disponibilizado (R$)' in df.columns:
                df_temp = df.copy()
                df_temp['Valor_Numerico'] = df_temp['Valor Disponibilizado (R$)'].str.replace(',', '.').str.replace('R$', '').str.strip()
                df_temp['Valor_Numerico'] = pd.to_numeric(df_temp['Valor_Numerico'], errors='coerce')
                
                total_valor = df_temp['Valor_Numerico'].sum()
                print(f"\nValor total disponibilizado: R$ {total_valor:,.2f}")
            
            # Mostrar primeiras linhas
            print(f"\nPrimeiras 5 linhas dos dados:")
            print(df.head().to_string())
            
            return True
            
        else:
            logger.error("Nenhum dado foi coletado")
            return False
    
    except Exception as e:
        logger.error(f"Erro geral durante a raspagem: {e}")
        return False
    
    finally:
        driver.quit()

if __name__ == "__main__":
    success = scrape_all_data()
    if success:
        print("\n✅ Raspagem concluída com sucesso!")
    else:
        print("\n❌ Falha na raspagem dos dados")
