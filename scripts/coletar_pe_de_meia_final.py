#!/usr/bin/env python3
"""
Script final corrigido para coletar TODOS os dados do Programa Pé-de-Meia
Versão com correção do mapeamento de colunas
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

def download_and_process_period(year, month):
    """
    Baixa e processa dados para um período específico
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
                        return None
            else:
                csv_content = content.decode('utf-8', errors='ignore')
            
            # Processar CSV diretamente
            df = pd.read_csv(io.StringIO(csv_content), 
                            sep=';', 
                            encoding='utf-8', 
                            low_memory=False,
                            dtype=str)
            
            log_message(f"Carregado: {len(df)} registros")
            log_message(f"Colunas originais: {list(df.columns)}")
            
            # Criar DataFrame final com mapeamento correto
            result_df = pd.DataFrame()
            
            # Mapear colunas baseado nos nomes reais encontrados
            result_df['Detalhar'] = f"https://portaldatransparencia.gov.br/beneficios/pe-de-meia/{year_month}"
            result_df['Mês Referência'] = f"{month:02d}/{year}"
            
            # Mapear UF
            if 'UF' in df.columns:
                result_df['UF'] = df['UF']
            else:
                result_df['UF'] = ""
            
            # Mapear Município
            if 'NOME MUNICPIO' in df.columns:
                result_df['Município'] = df['NOME MUNICPIO']
            elif 'NOME MUNICÍPIO' in df.columns:
                result_df['Município'] = df['NOME MUNICÍPIO']
            elif 'MUNICIPIO' in df.columns:
                result_df['Município'] = df['MUNICIPIO']
            else:
                result_df['Município'] = ""
            
            # Mapear Beneficiário
            if 'NOME BENEFICIRIO' in df.columns:
                result_df['Beneficiário'] = df['NOME BENEFICIRIO']
            elif 'NOME BENEFICIÁRIO' in df.columns:
                result_df['Beneficiário'] = df['NOME BENEFICIÁRIO']
            elif 'BENEFICIARIO' in df.columns:
                result_df['Beneficiário'] = df['BENEFICIARIO']
            else:
                result_df['Beneficiário'] = ""
            
            # Mapear CPF do Beneficiário
            if 'CPF BENEFICIRIO' in df.columns:
                result_df['CPF do Beneficiário'] = df['CPF BENEFICIRIO']
            elif 'CPF BENEFICIÁRIO' in df.columns:
                result_df['CPF do Beneficiário'] = df['CPF BENEFICIÁRIO']
            elif 'CPF' in df.columns:
                result_df['CPF do Beneficiário'] = df['CPF']
            else:
                result_df['CPF do Beneficiário'] = ""
            
            # Mapear Representante Legal
            if 'NOME RESPONSVEL' in df.columns:
                result_df['Representante Legal'] = df['NOME RESPONSVEL']
            elif 'NOME RESPONSÁVEL' in df.columns:
                result_df['Representante Legal'] = df['NOME RESPONSÁVEL']
            elif 'REPRESENTANTE_LEGAL' in df.columns:
                result_df['Representante Legal'] = df['REPRESENTANTE_LEGAL']
            else:
                result_df['Representante Legal'] = ""
            
            # Mapear Valor Disponibilizado
            if 'VALOR PARCELA' in df.columns:
                result_df['Valor Disponibilizado'] = df['VALOR PARCELA']
            elif 'VALOR_DISPONIBILIZADO' in df.columns:
                result_df['Valor Disponibilizado'] = df['VALOR_DISPONIBILIZADO']
            elif 'VALOR' in df.columns:
                result_df['Valor Disponibilizado'] = df['VALOR']
            else:
                result_df['Valor Disponibilizado'] = ""
            
            log_message(f"✓ Processado: {len(result_df)} registros para {year}/{month:02d}")
            return result_df
            
        else:
            log_message(f"✗ Erro {response.status_code} para {year}/{month:02d}")
            return None
            
    except Exception as e:
        log_message(f"✗ Erro ao processar {year}/{month:02d}: {e}")
        return None

def main():
    """
    Função principal corrigida
    """
    log_message("=== INICIANDO COLETA COMPLETA DOS DADOS PÉ-DE-MEIA (VERSÃO FINAL CORRIGIDA) ===")
    
    # Períodos para coletar (baseado nos dados disponíveis identificados)
    periods = [
        (2024, 1), (2024, 2), (2024, 3), (2024, 4), (2024, 5),
        (2024, 6), (2024, 7), (2024, 8), (2024, 9), (2024, 10),
        (2024, 11), (2024, 12), (2025, 1)
    ]
    
    log_message(f"Períodos a processar: {len(periods)}")
    
    all_data = []
    successful_downloads = 0
    
    for year, month in periods:
        df = download_and_process_period(year, month)
        
        if df is not None and len(df) > 0:
            all_data.append(df)
            successful_downloads += 1
            log_message(f"✓ Sucesso: {len(df)} registros coletados para {year}/{month:02d}")
        else:
            log_message(f"✗ Falha para {year}/{month:02d}")
        
        # Pausa entre requisições
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
        
        log_message(f"=== COLETA CONCLUÍDA COM SUCESSO ===")
        log_message(f"Arquivo salvo: {output_file}")
        log_message(f"Total de registros: {len(final_df)}")
        log_message(f"Downloads bem-sucedidos: {successful_downloads}/{len(periods)}")
        log_message(f"Colunas: {list(final_df.columns)}")
        
        # Mostrar estatísticas
        log_message("=== ESTATÍSTICAS FINAIS ===")
        log_message(f"Estados únicos: {final_df['UF'].nunique()}")
        log_message(f"Municípios únicos: {final_df['Município'].nunique()}")
        log_message(f"Beneficiários únicos: {final_df['CPF do Beneficiário'].nunique()}")
        
        # Mostrar amostra dos dados
        log_message("=== AMOSTRA DOS DADOS ===")
        log_message(f"Primeiras 3 linhas:")
        for i, row in final_df.head(3).iterrows():
            log_message(f"Linha {i+1}: UF={row['UF']}, Município={row['Município'][:30]}..., Beneficiário={row['Beneficiário'][:30]}...")
        
        return True
    else:
        log_message("✗ ERRO: Nenhum dado foi coletado com sucesso")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
