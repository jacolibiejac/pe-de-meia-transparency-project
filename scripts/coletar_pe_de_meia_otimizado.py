#!/usr/bin/env python3
"""
Script otimizado para coletar TODOS os dados do Programa Pé-de-Meia
Versão melhorada com tratamento de memória e processamento em lotes
"""

import requests
import pandas as pd
import time
import os
from datetime import datetime
import zipfile
import io
import sys

def log_message(message):
    """Log com timestamp"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")
    sys.stdout.flush()

def download_and_save_data(year, month, output_dir="/home/ubuntu/pe_de_meia_temp"):
    """
    Baixa e salva dados para um período específico
    """
    # Criar diretório temporário se não existir
    os.makedirs(output_dir, exist_ok=True)
    
    year_month = f"{year}{month:02d}"
    url = f"https://portaldatransparencia.gov.br/download-de-dados/pe-de-meia/{year_month}"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
        'Connection': 'keep-alive',
    }
    
    try:
        log_message(f"Baixando dados para {year}/{month:02d}...")
        response = requests.get(url, headers=headers, timeout=120, stream=True)
        
        if response.status_code == 200:
            content = response.content
            
            # Verificar se é ZIP
            if content.startswith(b'PK'):
                log_message("Arquivo ZIP detectado, extraindo...")
                with zipfile.ZipFile(io.BytesIO(content)) as zip_file:
                    csv_files = [f for f in zip_file.namelist() if f.endswith('.csv')]
                    if csv_files:
                        csv_content = zip_file.read(csv_files[0]).decode('utf-8', errors='ignore')
                    else:
                        log_message("Nenhum CSV encontrado no ZIP")
                        return False
            else:
                csv_content = content.decode('utf-8', errors='ignore')
            
            # Salvar arquivo temporário
            temp_file = os.path.join(output_dir, f"pe_de_meia_{year_month}.csv")
            with open(temp_file, 'w', encoding='utf-8') as f:
                f.write(csv_content)
            
            log_message(f"✓ Dados salvos: {temp_file}")
            return temp_file
            
        else:
            log_message(f"✗ Erro {response.status_code} para {year}/{month:02d}")
            return False
            
    except Exception as e:
        log_message(f"✗ Erro ao baixar {year}/{month:02d}: {e}")
        return False

def process_single_file(file_path, year, month):
    """
    Processa um único arquivo CSV
    """
    try:
        log_message(f"Processando {file_path}...")
        
        # Ler com configurações otimizadas
        df = pd.read_csv(file_path, 
                        sep=';', 
                        encoding='utf-8', 
                        low_memory=False,
                        dtype=str)  # Forçar tudo como string para evitar problemas
        
        log_message(f"Carregado: {len(df)} registros, colunas: {list(df.columns)}")
        
        # Mapear colunas (case-insensitive)
        df.columns = df.columns.str.upper().str.strip()
        
        column_mapping = {
            'MES_REFERENCIA': 'Mês Referência',
            'MÊS_REFERÊNCIA': 'Mês Referência', 
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
        
        # Renomear colunas encontradas
        df = df.rename(columns=column_mapping)
        
        # Criar DataFrame final com as 8 colunas necessárias
        final_columns = [
            'Detalhar',
            'Mês Referência', 
            'UF', 
            'Município', 
            'Beneficiário', 
            'CPF do Beneficiário', 
            'Representante Legal', 
            'Valor Disponibilizado'
        ]
        
        result_df = pd.DataFrame()
        
        for col in final_columns:
            if col in df.columns:
                result_df[col] = df[col]
            elif col == 'Detalhar':
                result_df[col] = f"https://portaldatransparencia.gov.br/beneficios/pe-de-meia/{year}{month:02d}"
            elif col == 'Mês Referência':
                result_df[col] = f"{month:02d}/{year}"
            else:
                result_df[col] = ""
        
        log_message(f"✓ Processado: {len(result_df)} registros para {year}/{month:02d}")
        return result_df
        
    except Exception as e:
        log_message(f"✗ Erro ao processar {file_path}: {e}")
        return None

def main():
    """
    Função principal otimizada
    """
    log_message("=== INICIANDO COLETA COMPLETA DOS DADOS PÉ-DE-MEIA (VERSÃO OTIMIZADA) ===")
    
    # Períodos para coletar
    periods = []
    
    # 2024 - todos os meses
    for month in range(1, 13):
        periods.append((2024, month))
    
    # 2025 - até setembro
    for month in range(1, 10):
        periods.append((2025, month))
    
    log_message(f"Total de períodos: {len(periods)}")
    
    # Fase 1: Download de todos os arquivos
    log_message("=== FASE 1: DOWNLOAD DOS ARQUIVOS ===")
    downloaded_files = []
    
    for year, month in periods:
        file_path = download_and_save_data(year, month)
        if file_path:
            downloaded_files.append((file_path, year, month))
        time.sleep(1)  # Pausa menor entre downloads
    
    log_message(f"Downloads concluídos: {len(downloaded_files)}/{len(periods)}")
    
    if not downloaded_files:
        log_message("✗ ERRO: Nenhum arquivo foi baixado com sucesso")
        return False
    
    # Fase 2: Processamento dos arquivos
    log_message("=== FASE 2: PROCESSAMENTO DOS DADOS ===")
    
    # Processar em lotes para economizar memória
    batch_size = 5
    all_processed_files = []
    
    for i in range(0, len(downloaded_files), batch_size):
        batch = downloaded_files[i:i+batch_size]
        log_message(f"Processando lote {i//batch_size + 1}/{(len(downloaded_files)-1)//batch_size + 1}")
        
        batch_data = []
        for file_path, year, month in batch:
            df = process_single_file(file_path, year, month)
            if df is not None:
                batch_data.append(df)
        
        if batch_data:
            # Consolidar lote
            batch_df = pd.concat(batch_data, ignore_index=True)
            
            # Salvar lote processado
            batch_file = f"/home/ubuntu/pe_de_meia_batch_{i//batch_size + 1}.csv"
            batch_df.to_csv(batch_file, index=False, encoding='utf-8', sep=';')
            all_processed_files.append(batch_file)
            
            log_message(f"Lote salvo: {batch_file} ({len(batch_df)} registros)")
            
            # Limpar memória
            del batch_data, batch_df
    
    # Fase 3: Consolidação final
    log_message("=== FASE 3: CONSOLIDAÇÃO FINAL ===")
    
    final_data = []
    for batch_file in all_processed_files:
        log_message(f"Carregando {batch_file}...")
        df = pd.read_csv(batch_file, sep=';', encoding='utf-8', dtype=str)
        final_data.append(df)
    
    # Consolidar tudo
    log_message("Consolidando dados finais...")
    final_df = pd.concat(final_data, ignore_index=True)
    
    # Remover duplicatas
    initial_count = len(final_df)
    final_df = final_df.drop_duplicates(subset=['CPF do Beneficiário', 'Mês Referência'], keep='first')
    final_count = len(final_df)
    
    log_message(f"Registros antes da remoção de duplicatas: {initial_count}")
    log_message(f"Registros após remoção de duplicatas: {final_count}")
    
    # Salvar arquivo final
    output_file = "/home/ubuntu/dados_portal_transparencia_completo.csv"
    final_df.to_csv(output_file, index=False, encoding='utf-8', sep=';')
    
    log_message("=== COLETA CONCLUÍDA COM SUCESSO ===")
    log_message(f"Arquivo final: {output_file}")
    log_message(f"Total de registros: {len(final_df)}")
    log_message(f"Colunas: {list(final_df.columns)}")
    
    # Estatísticas
    log_message("=== ESTATÍSTICAS FINAIS ===")
    log_message(f"Estados únicos: {final_df['UF'].nunique()}")
    log_message(f"Municípios únicos: {final_df['Município'].nunique()}")
    log_message(f"Beneficiários únicos: {final_df['CPF do Beneficiário'].nunique()}")
    
    # Limpeza de arquivos temporários
    log_message("Limpando arquivos temporários...")
    import shutil
    if os.path.exists("/home/ubuntu/pe_de_meia_temp"):
        shutil.rmtree("/home/ubuntu/pe_de_meia_temp")
    
    for batch_file in all_processed_files:
        if os.path.exists(batch_file):
            os.remove(batch_file)
    
    log_message("✓ Limpeza concluída")
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
