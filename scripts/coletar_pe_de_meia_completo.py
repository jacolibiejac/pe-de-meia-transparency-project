#!/usr/bin/env python3
"""
Script para coletar TODOS os dados do Programa Pé-de-Meia do Portal da Transparência
Baseado na análise das requisições de rede identificadas
"""

import requests
import pandas as pd
import time
import os
from datetime import datetime, timedelta
import zipfile
import io
import sys

def log_message(message):
    """Log com timestamp"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

def download_pe_de_meia_data(year, month):
    """
    Baixa dados do Pé-de-Meia para um ano/mês específico
    Baseado no padrão identificado: /download-de-dados/pe-de-meia/YYYYMM
    """
    # Formatar ano/mês no padrão YYYYMM
    year_month = f"{year}{month:02d}"
    
    # URL baseada no padrão identificado
    url = f"https://portaldatransparencia.gov.br/download-de-dados/pe-de-meia/{year_month}"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }
    
    try:
        log_message(f"Baixando dados para {year}/{month:02d}...")
        response = requests.get(url, headers=headers, timeout=60, stream=True)
        
        if response.status_code == 200:
            # Verificar se é um arquivo ZIP ou CSV
            content_type = response.headers.get('content-type', '').lower()
            content_disposition = response.headers.get('content-disposition', '')
            
            log_message(f"Content-Type: {content_type}")
            log_message(f"Content-Disposition: {content_disposition}")
            
            # Ler o conteúdo
            content = response.content
            
            # Se for um arquivo ZIP, extrair
            if 'zip' in content_type or content.startswith(b'PK'):
                log_message("Arquivo ZIP detectado, extraindo...")
                with zipfile.ZipFile(io.BytesIO(content)) as zip_file:
                    # Listar arquivos no ZIP
                    file_list = zip_file.namelist()
                    log_message(f"Arquivos no ZIP: {file_list}")
                    
                    # Procurar por arquivo CSV
                    csv_files = [f for f in file_list if f.endswith('.csv')]
                    if csv_files:
                        csv_content = zip_file.read(csv_files[0])
                        return csv_content.decode('utf-8', errors='ignore')
                    else:
                        log_message("Nenhum arquivo CSV encontrado no ZIP")
                        return None
            else:
                # Assumir que é CSV direto
                return content.decode('utf-8', errors='ignore')
                
        else:
            log_message(f"Erro ao baixar {year}/{month:02d}: Status {response.status_code}")
            return None
            
    except Exception as e:
        log_message(f"Erro ao baixar dados para {year}/{month:02d}: {e}")
        return None

def process_csv_data(csv_content, year, month):
    """
    Processa o conteúdo CSV e padroniza as colunas
    """
    try:
        # Ler CSV
        df = pd.read_csv(io.StringIO(csv_content), sep=';', encoding='utf-8')
        
        log_message(f"Dados carregados: {len(df)} registros, {len(df.columns)} colunas")
        log_message(f"Colunas encontradas: {list(df.columns)}")
        
        # Mapear colunas para o formato padrão (8 colunas)
        # Baseado no formato já estabelecido
        column_mapping = {
            # Possíveis variações de nomes de colunas
            'MES_REFERENCIA': 'Mês Referência',
            'MÊS_REFERÊNCIA': 'Mês Referência',
            'MES_REF': 'Mês Referência',
            'MESREFERENCIA': 'Mês Referência',
            'UF': 'UF',
            'ESTADO': 'UF',
            'MUNICIPIO': 'Município',
            'MUNICÍPIO': 'Município',
            'NOME_BENEFICIARIO': 'Beneficiário',
            'BENEFICIARIO': 'Beneficiário',
            'NOME_BENEFICIÁRIO': 'Beneficiário',
            'CPF_BENEFICIARIO': 'CPF do Beneficiário',
            'CPF_BENEFICIÁRIO': 'CPF do Beneficiário',
            'CPF': 'CPF do Beneficiário',
            'REPRESENTANTE_LEGAL': 'Representante Legal',
            'VALOR_DISPONIBILIZADO': 'Valor Disponibilizado',
            'VALOR': 'Valor Disponibilizado',
        }
        
        # Renomear colunas
        df_renamed = df.rename(columns=column_mapping)
        
        # Garantir que temos as 8 colunas necessárias
        required_columns = [
            'Detalhar',
            'Mês Referência', 
            'UF', 
            'Município', 
            'Beneficiário', 
            'CPF do Beneficiário', 
            'Representante Legal', 
            'Valor Disponibilizado'
        ]
        
        # Criar DataFrame final com as colunas necessárias
        final_df = pd.DataFrame()
        
        for col in required_columns:
            if col in df_renamed.columns:
                final_df[col] = df_renamed[col]
            elif col == 'Detalhar':
                # Criar coluna Detalhar como link (se não existir)
                final_df[col] = f"https://portaldatransparencia.gov.br/beneficios/pe-de-meia/{year}{month:02d}"
            elif col == 'Mês Referência':
                # Se não tiver mês referência, usar o mês/ano atual
                final_df[col] = f"{month:02d}/{year}"
            else:
                # Preencher com vazio se não encontrar
                final_df[col] = ""
        
        log_message(f"Dados processados: {len(final_df)} registros finais")
        return final_df
        
    except Exception as e:
        log_message(f"Erro ao processar CSV: {e}")
        return None

def main():
    """
    Função principal para coletar todos os dados
    """
    log_message("=== INICIANDO COLETA COMPLETA DOS DADOS PÉ-DE-MEIA ===")
    
    # Definir períodos para coleta
    # Baseado na pesquisa: dados disponíveis para 2024 e 2025
    periods_to_collect = []
    
    # 2024 - todos os meses
    for month in range(1, 13):
        periods_to_collect.append((2024, month))
    
    # 2025 - até setembro (data atual)
    for month in range(1, 10):
        periods_to_collect.append((2025, month))
    
    log_message(f"Períodos a coletar: {len(periods_to_collect)}")
    
    all_data = []
    successful_downloads = 0
    
    for year, month in periods_to_collect:
        log_message(f"--- Processando {year}/{month:02d} ---")
        
        # Baixar dados
        csv_content = download_pe_de_meia_data(year, month)
        
        if csv_content:
            # Processar dados
            df = process_csv_data(csv_content, year, month)
            
            if df is not None and len(df) > 0:
                all_data.append(df)
                successful_downloads += 1
                log_message(f"✓ Sucesso: {len(df)} registros coletados para {year}/{month:02d}")
            else:
                log_message(f"✗ Falha no processamento para {year}/{month:02d}")
        else:
            log_message(f"✗ Falha no download para {year}/{month:02d}")
        
        # Pausa entre requisições para não sobrecarregar o servidor
        time.sleep(2)
    
    # Consolidar todos os dados
    if all_data:
        log_message("=== CONSOLIDANDO DADOS ===")
        final_df = pd.concat(all_data, ignore_index=True)
        
        # Remover duplicatas baseado em CPF e Mês Referência
        initial_count = len(final_df)
        final_df = final_df.drop_duplicates(subset=['CPF do Beneficiário', 'Mês Referência'], keep='first')
        final_count = len(final_df)
        
        log_message(f"Registros antes da remoção de duplicatas: {initial_count}")
        log_message(f"Registros após remoção de duplicatas: {final_count}")
        log_message(f"Duplicatas removidas: {initial_count - final_count}")
        
        # Salvar arquivo final
        output_file = "/home/ubuntu/dados_portal_transparencia_completo.csv"
        final_df.to_csv(output_file, index=False, encoding='utf-8', sep=';')
        
        log_message(f"=== COLETA CONCLUÍDA ===")
        log_message(f"Arquivo salvo: {output_file}")
        log_message(f"Total de registros: {len(final_df)}")
        log_message(f"Downloads bem-sucedidos: {successful_downloads}/{len(periods_to_collect)}")
        log_message(f"Colunas: {list(final_df.columns)}")
        
        # Mostrar estatísticas
        log_message("=== ESTATÍSTICAS ===")
        log_message(f"Estados únicos: {final_df['UF'].nunique()}")
        log_message(f"Municípios únicos: {final_df['Município'].nunique()}")
        log_message(f"Beneficiários únicos: {final_df['CPF do Beneficiário'].nunique()}")
        
        if 'Valor Disponibilizado' in final_df.columns:
            # Converter valores para numérico
            final_df['Valor Disponibilizado'] = pd.to_numeric(
                final_df['Valor Disponibilizado'].astype(str).str.replace(',', '.').str.replace(r'[^\d.]', '', regex=True), 
                errors='coerce'
            )
            total_value = final_df['Valor Disponibilizado'].sum()
            log_message(f"Valor total disponibilizado: R$ {total_value:,.2f}")
        
        return True
    else:
        log_message("✗ ERRO: Nenhum dado foi coletado com sucesso")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
