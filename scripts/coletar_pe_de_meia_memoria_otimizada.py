#!/usr/bin/env python3
"""
Script ultra-otimizado para coletar dados do Programa Pé-de-Meia
Versão com processamento chunk-by-chunk para economizar memória
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

def process_period_to_file(year, month, output_file):
    """
    Baixa e processa dados para um período específico, salvando diretamente no arquivo final
    """
    year_month = f"{year}{month:02d}"
    url = f"https://portaldatransparencia.gov.br/download-de-dados/pe-de-meia/{year_month}"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'pt-BR,pt;q=0.9,en;q=0.8',
        'Connection': 'keep-alive',
    }
    
    try:
        log_message(f"Processando {year}/{month:02d}...")
        response = requests.get(url, headers=headers, timeout=120, stream=True)
        
        if response.status_code == 200:
            content = response.content
            
            # Verificar se é ZIP
            if content.startswith(b'PK'):
                with zipfile.ZipFile(io.BytesIO(content)) as zip_file:
                    csv_files = [f for f in zip_file.namelist() if f.endswith('.csv')]
                    if csv_files:
                        csv_content = zip_file.read(csv_files[0]).decode('utf-8', errors='ignore')
                    else:
                        log_message(f"✗ Nenhum CSV encontrado no ZIP para {year}/{month:02d}")
                        return 0
            else:
                csv_content = content.decode('utf-8', errors='ignore')
            
            # Processar em chunks para economizar memória
            chunk_size = 50000  # Processar 50k registros por vez
            total_processed = 0
            
            # Verificar se arquivo de saída já existe para determinar se precisa escrever cabeçalho
            write_header = not os.path.exists(output_file)
            
            # Processar CSV em chunks
            csv_reader = pd.read_csv(io.StringIO(csv_content), 
                                   sep=';', 
                                   encoding='utf-8', 
                                   dtype=str,
                                   chunksize=chunk_size)
            
            for chunk_num, chunk in enumerate(csv_reader):
                log_message(f"Processando chunk {chunk_num + 1} de {year}/{month:02d} ({len(chunk)} registros)")
                
                # Criar DataFrame resultado para este chunk
                result_chunk = pd.DataFrame()
                
                # Mapear colunas
                result_chunk['Detalhar'] = f"https://portaldatransparencia.gov.br/beneficios/pe-de-meia/{year_month}"
                result_chunk['Mês Referência'] = f"{month:02d}/{year}"
                result_chunk['UF'] = chunk.get('UF', '')
                result_chunk['Município'] = chunk.get('NOME MUNICPIO', chunk.get('NOME MUNICÍPIO', ''))
                result_chunk['Beneficiário'] = chunk.get('NOME BENEFICIRIO', chunk.get('NOME BENEFICIÁRIO', ''))
                result_chunk['CPF do Beneficiário'] = chunk.get('CPF BENEFICIRIO', chunk.get('CPF BENEFICIÁRIO', ''))
                result_chunk['Representante Legal'] = chunk.get('NOME RESPONSVEL', chunk.get('NOME RESPONSÁVEL', ''))
                result_chunk['Valor Disponibilizado'] = chunk.get('VALOR PARCELA', chunk.get('VALOR_DISPONIBILIZADO', ''))
                
                # Salvar chunk no arquivo final
                result_chunk.to_csv(output_file, 
                                  mode='a',  # Append mode
                                  header=write_header,  # Escrever cabeçalho apenas na primeira vez
                                  index=False, 
                                  encoding='utf-8', 
                                  sep=';')
                
                write_header = False  # Não escrever cabeçalho nos próximos chunks
                total_processed += len(result_chunk)
                
                # Limpar memória
                del result_chunk, chunk
            
            log_message(f"✓ Processado: {total_processed} registros para {year}/{month:02d}")
            return total_processed
            
        else:
            log_message(f"✗ Erro {response.status_code} para {year}/{month:02d}")
            return 0
            
    except Exception as e:
        log_message(f"✗ Erro ao processar {year}/{month:02d}: {e}")
        return 0

def remove_duplicates_from_file(input_file, output_file):
    """
    Remove duplicatas do arquivo final processando em chunks
    """
    log_message("=== REMOVENDO DUPLICATAS ===")
    
    seen_combinations = set()
    chunk_size = 100000
    total_original = 0
    total_unique = 0
    
    # Ler arquivo em chunks e escrever apenas registros únicos
    write_header = True
    
    try:
        for chunk_num, chunk in enumerate(pd.read_csv(input_file, sep=';', encoding='utf-8', dtype=str, chunksize=chunk_size)):
            log_message(f"Processando chunk {chunk_num + 1} para remoção de duplicatas ({len(chunk)} registros)")
            
            total_original += len(chunk)
            
            # Criar chave única baseada em CPF e Mês Referência
            chunk['unique_key'] = chunk['CPF do Beneficiário'].astype(str) + '|' + chunk['Mês Referência'].astype(str)
            
            # Filtrar apenas registros únicos
            unique_chunk = chunk[~chunk['unique_key'].isin(seen_combinations)]
            
            # Adicionar novas combinações ao conjunto
            seen_combinations.update(unique_chunk['unique_key'].tolist())
            
            # Remover coluna auxiliar
            unique_chunk = unique_chunk.drop('unique_key', axis=1)
            
            if len(unique_chunk) > 0:
                # Salvar chunk único
                unique_chunk.to_csv(output_file, 
                                  mode='a',
                                  header=write_header,
                                  index=False, 
                                  encoding='utf-8', 
                                  sep=';')
                write_header = False
                total_unique += len(unique_chunk)
            
            # Limpar memória
            del chunk, unique_chunk
        
        log_message(f"Registros originais: {total_original}")
        log_message(f"Registros únicos: {total_unique}")
        log_message(f"Duplicatas removidas: {total_original - total_unique}")
        
        return total_unique
        
    except Exception as e:
        log_message(f"Erro na remoção de duplicatas: {e}")
        return 0

def main():
    """
    Função principal ultra-otimizada
    """
    log_message("=== INICIANDO COLETA COMPLETA DOS DADOS PÉ-DE-MEIA (VERSÃO ULTRA-OTIMIZADA) ===")
    
    # Períodos para coletar
    periods = [
        (2024, 1), (2024, 2), (2024, 3), (2024, 4), (2024, 5),
        (2024, 6), (2024, 7), (2024, 8), (2024, 9), (2024, 10),
        (2024, 11), (2024, 12), (2025, 1)
    ]
    
    log_message(f"Períodos a processar: {len(periods)}")
    
    # Arquivo temporário para dados brutos
    temp_file = "/home/ubuntu/dados_pe_de_meia_temp.csv"
    final_file = "/home/ubuntu/dados_portal_transparencia_completo.csv"
    
    # Remover arquivos existentes
    for file_path in [temp_file, final_file]:
        if os.path.exists(file_path):
            os.remove(file_path)
    
    total_records = 0
    successful_downloads = 0
    
    # Processar cada período diretamente no arquivo
    for year, month in periods:
        records = process_period_to_file(year, month, temp_file)
        
        if records > 0:
            total_records += records
            successful_downloads += 1
            log_message(f"✓ Sucesso: {records} registros coletados para {year}/{month:02d}")
        else:
            log_message(f"✗ Falha para {year}/{month:02d}")
        
        # Pausa entre requisições
        time.sleep(2)
    
    if total_records > 0:
        log_message(f"=== DADOS COLETADOS ===")
        log_message(f"Total de registros brutos: {total_records}")
        log_message(f"Downloads bem-sucedidos: {successful_downloads}/{len(periods)}")
        
        # Remover duplicatas
        unique_records = remove_duplicates_from_file(temp_file, final_file)
        
        if unique_records > 0:
            log_message(f"=== COLETA CONCLUÍDA COM SUCESSO ===")
            log_message(f"Arquivo final: {final_file}")
            log_message(f"Total de registros únicos: {unique_records}")
            
            # Mostrar estatísticas básicas
            log_message("=== ESTATÍSTICAS BÁSICAS ===")
            try:
                # Ler apenas uma amostra para estatísticas
                sample = pd.read_csv(final_file, sep=';', encoding='utf-8', dtype=str, nrows=10000)
                log_message(f"Estados únicos (amostra): {sample['UF'].nunique()}")
                log_message(f"Municípios únicos (amostra): {sample['Município'].nunique()}")
                log_message(f"Colunas: {list(sample.columns)}")
                
                # Mostrar amostra
                log_message("=== AMOSTRA DOS DADOS ===")
                for i, row in sample.head(3).iterrows():
                    log_message(f"Linha {i+1}: UF={row['UF']}, Município={str(row['Município'])[:30]}..., Beneficiário={str(row['Beneficiário'])[:30]}...")
                
            except Exception as e:
                log_message(f"Erro ao calcular estatísticas: {e}")
            
            # Limpar arquivo temporário
            if os.path.exists(temp_file):
                os.remove(temp_file)
            
            return True
        else:
            log_message("✗ ERRO: Falha na remoção de duplicatas")
            return False
    else:
        log_message("✗ ERRO: Nenhum dado foi coletado com sucesso")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
