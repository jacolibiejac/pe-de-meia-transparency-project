// Script JavaScript para extrair todos os dados do Portal da Transparência
async function extractAllData() {
    console.log('Iniciando extração de dados...');
    
    let allData = [];
    let headers = [];
    let pageNum = 1;
    const maxRecords = 20000;
    
    // Função para aguardar
    function sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
    
    // Função para extrair dados da página atual
    function extractCurrentPageData() {
        const table = document.querySelector('table');
        if (!table) {
            console.log('Tabela não encontrada');
            return { headers: [], data: [] };
        }
        
        // Extrair cabeçalhos (apenas na primeira vez)
        if (headers.length === 0) {
            const headerElements = table.querySelectorAll('thead th');
            headers = Array.from(headerElements).map(th => th.textContent.trim());
            console.log('Cabeçalhos encontrados:', headers);
        }
        
        // Extrair dados das linhas
        const rows = Array.from(table.querySelectorAll('tbody tr'));
        const data = rows.map(row => {
            const cells = Array.from(row.querySelectorAll('td'));
            return cells.map(cell => cell.textContent.trim());
        });
        
        return { headers: headers, data: data };
    }
    
    // Função para verificar se há próxima página e navegar
    function navigateToNextPage() {
        const nextButton = document.querySelector('#lista_next button');
        if (!nextButton) {
            console.log('Botão próxima página não encontrado');
            return false;
        }
        
        const parentLi = nextButton.parentElement;
        if (parentLi.classList.contains('disabled')) {
            console.log('Última página atingida');
            return false;
        }
        
        nextButton.click();
        return true;
    }
    
    try {
        // Extrair dados da primeira página
        let result = extractCurrentPageData();
        headers = result.headers;
        
        if (result.data.length > 0) {
            allData.push(...result.data);
            console.log(`Página ${pageNum}: ${result.data.length} registros. Total: ${allData.length}`);
        }
        
        // Navegar pelas próximas páginas
        while (allData.length < maxRecords) {
            // Tentar navegar para próxima página
            if (!navigateToNextPage()) {
                break;
            }
            
            // Aguardar a página carregar
            await sleep(3000);
            pageNum++;
            
            // Extrair dados da nova página
            result = extractCurrentPageData();
            
            if (result.data.length === 0) {
                console.log('Nenhum dado encontrado na página, parando...');
                break;
            }
            
            allData.push(...result.data);
            console.log(`Página ${pageNum}: ${result.data.length} registros. Total: ${allData.length}`);
            
            // Verificar limite
            if (allData.length >= maxRecords) {
                allData = allData.slice(0, maxRecords);
                console.log(`Limite de ${maxRecords} registros atingido!`);
                break;
            }
        }
        
        console.log(`Extração concluída! Total: ${allData.length} registros em ${pageNum} páginas`);
        
        return {
            success: true,
            headers: headers,
            data: allData,
            totalRecords: allData.length,
            totalPages: pageNum
        };
        
    } catch (error) {
        console.error('Erro durante a extração:', error);
        return {
            success: false,
            error: error.message,
            headers: headers,
            data: allData,
            totalRecords: allData.length
        };
    }
}

// Executar a extração
extractAllData().then(result => {
    console.log('Resultado final:', result);
    
    // Salvar resultado no localStorage para recuperar depois
    localStorage.setItem('portalTransparenciaData', JSON.stringify(result));
    console.log('Dados salvos no localStorage com a chave "portalTransparenciaData"');
});
