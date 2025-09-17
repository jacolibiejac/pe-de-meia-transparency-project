import pandas as pd
import json
import sys
from datetime import datetime

def get_data_from_browser():
    """
    Este script deve ser executado após navegar para uma nova página no navegador.
    Ele irá extrair os dados da página atual e adicionar ao arquivo CSV existente.
    """
    
    print("="*60)
    print("COLETANDO DADOS DA PÁGINA ATUAL...")
    print("="*60)
    
    # Instruções para o usuário
    print("INSTRUÇÕES:")
    print("1. Certifique-se de que está na página correta do Portal da Transparência")
    print("2. Abra o console do navegador (F12 > Console)")
    print("3. Cole e execute o seguinte código JavaScript:")
    print()
    print("// CÓDIGO JAVASCRIPT PARA COLAR NO CONSOLE:")
    print("// ==========================================")
    
    js_code = """
(() => {
    const table = document.querySelector('table');
    if (!table) {
        console.log('ERRO: Tabela não encontrada');
        return { error: 'Tabela não encontrada' };
    }
    
    const rows = Array.from(table.querySelectorAll('tbody tr'));
    const data = rows.map(row => {
        const cells = Array.from(row.querySelectorAll('td'));
        return cells.map(cell => cell.textContent.trim());
    });
    
    console.log('Dados extraídos:', data.length, 'registros');
    console.log('Copie o resultado abaixo e cole no arquivo page_data.json:');
    console.log(JSON.stringify(data, null, 2));
    
    return data;
})()
"""
    
    print(js_code)
    print("// ==========================================")
    print()
    print("4. Copie o resultado JSON e salve no arquivo 'page_data.json'")
    print("5. Execute novamente este script Python")
    print()
    
    # Verificar se existe arquivo com dados da página
    try:
        with open('/home/ubuntu/page_data.json', 'r', encoding='utf-8') as f:
            page_data = json.load(f)
        
        if not page_data:
            print("❌ Nenhum dado encontrado no arquivo page_data.json")
            return False
        
        print(f"✅ Dados encontrados: {len(page_data)} registros")
        
        # Carregar dados existentes
        try:
            existing_df = pd.read_csv("/home/ubuntu/dados_portal_transparencia.csv")
            print(f"📊 Registros existentes: {len(existing_df):,}")
        except FileNotFoundError:
            print("❌ Arquivo CSV principal não encontrado. Execute primeiro: python collect_all_data.py")
            return False
        
        # Verificar se já atingiu o limite
        if len(existing_df) >= 20000:
            print("⚠️  Limite de 20.000 registros já atingido!")
            return False
        
        # Preparar novos dados
        headers = existing_df.columns.tolist()
        
        # Filtrar e limpar novos dados
        clean_data = []
        for row in page_data:
            if len(row) >= len(headers) and any(cell.strip() for cell in row):
                # Ajustar tamanho da linha
                if len(row) > len(headers):
                    row = row[:len(headers)]
                elif len(row) < len(headers):
                    row.extend([''] * (len(headers) - len(row)))
                clean_data.append(row)
        
        if not clean_data:
            print("❌ Nenhum dado válido encontrado")
            return False
        
        # Criar DataFrame com novos dados
        new_df = pd.DataFrame(clean_data, columns=headers)
        
        # Combinar com dados existentes
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        
        # Aplicar limite de 20.000 registros
        if len(combined_df) > 20000:
            combined_df = combined_df.head(20000)
            print(f"⚠️  Dados limitados a 20.000 registros")
        
        # Salvar arquivo atualizado
        output_file = "/home/ubuntu/dados_portal_transparencia.csv"
        combined_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        
        print(f"✅ Dados atualizados com sucesso!")
        print(f"📁 Arquivo: {output_file}")
        print(f"📊 Total de registros: {len(combined_df):,}")
        print(f"📈 Novos registros adicionados: {len(new_df):,}")
        print(f"🎯 Progresso: {len(combined_df)/20000*100:.1f}% do limite máximo")
        
        # Mostrar estatísticas atualizadas
        print(f"\n📈 ESTATÍSTICAS ATUALIZADAS:")
        print(f"Distribuição por UF (Top 10):")
        uf_counts = combined_df['UF'].value_counts().head(10)
        for uf, count in uf_counts.items():
            print(f"  {uf}: {count:,} registros")
        
        # Valor total
        df_temp = combined_df.copy()
        df_temp['Valor_Numerico'] = df_temp['Valor Disponibilizado (R$)'].str.replace(',', '.').str.replace('R$', '').str.strip()
        df_temp['Valor_Numerico'] = pd.to_numeric(df_temp['Valor_Numerico'], errors='coerce')
        total_valor = df_temp['Valor_Numerico'].sum()
        print(f"\n💰 Valor total acumulado: R$ {total_valor:,.2f}")
        
        # Remover arquivo temporário
        import os
        try:
            os.remove('/home/ubuntu/page_data.json')
            print("🗑️  Arquivo temporário removido")
        except:
            pass
        
        return True
        
    except FileNotFoundError:
        print("❌ Arquivo 'page_data.json' não encontrado.")
        print("Execute o código JavaScript no console do navegador primeiro.")
        return False
    except json.JSONDecodeError:
        print("❌ Erro ao ler arquivo JSON. Verifique o formato dos dados.")
        return False

if __name__ == "__main__":
    success = get_data_from_browser()
    
    if success:
        print("\n🎉 Página processada com sucesso!")
        print("➡️  Para continuar: navegue para a próxima página e execute este script novamente")
    else:
        print("\n❌ Falha ao processar a página")
