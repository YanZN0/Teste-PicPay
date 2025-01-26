import asyncio
import aiohttp
import pandas as pd


# Função para obter os dados do Pokémon
async def obter_pokemon_API(session, pokemon_name):
    url = f"https://pokeapi.co/api/v2/pokemon/{pokemon_name}/"
    async with session.get(url) as response:
        if response.status == 200:
            data = await response.json()
            # Filtrar os atributos principais (hp, attack, defense, speed)
            stats = {
                stat['stat']['name']: stat['base_stat']
                for stat in data['stats']
                if stat['stat']['name'] in ['hp', 'attack', 'defense', 'speed']
            }
            return {'name': data['name'], 'stats': stats}
        return None


# Função para obter a cadeia evolutiva do Pokémon
async def obter_cadeia_evolutiva(session, pokemon_id):
    url = f"https://pokeapi.co/api/v2/pokemon-species/{pokemon_id}/"
    async with session.get(url) as response:
        if response.status == 200:
            species_data = await response.json()
            evo_chain_url = species_data['evolution_chain']['url']
            async with session.get(evo_chain_url) as response:
                if response.status == 200:
                    return await response.json()
        return None


# Função para processar os dados dos Pokémons e calcular os aumentos de atributos
async def processar_dados_pokemon(session, pokemon_id, processados, melhor_evolucao):
    cadeia = await obter_cadeia_evolutiva(session, pokemon_id)
    if not cadeia:
        return

    evolucoes = []
    cadeia_atual = cadeia['chain']
    while cadeia_atual:
        evolucoes.append(cadeia_atual['species']['name'])
        cadeia_atual = cadeia_atual['evolves_to'][0] if cadeia_atual['evolves_to'] else None

    for i in range(len(evolucoes) - 1):
        pre_evo = evolucoes[i]
        evo = evolucoes[i + 1]

        # Evitar processar o mesmo par várias vezes
        if (pre_evo, evo) in processados:
            continue

        pokemon_atual = await obter_pokemon_API(session, pre_evo)
        proxima_evolucao = await obter_pokemon_API(session, evo)

        if not pokemon_atual or not proxima_evolucao:
            continue

        # Calcular o aumento de atributos
        for atributo, valor_base in pokemon_atual['stats'].items():
            aumento = proxima_evolucao['stats'].get(atributo, 0) - valor_base
            if aumento > 0:
                # Verificar se é a maior evolução para o Pokémon atual
                if pre_evo not in melhor_evolucao or aumento > melhor_evolucao[pre_evo]['Aumento']:
                    melhor_evolucao[pre_evo] = {
                        'Pré-evolução': pre_evo,
                        'Evolução': evo,
                        'Atributo': atributo,
                        'Aumento': aumento
                    }

        processados.add((pre_evo, evo))


# Função para calcular os maiores aumentos de atributos
async def calcular_maiores_aumentos_atributos():
    async with aiohttp.ClientSession() as session:
        aumentos = []
        processados = set()  # Para evitar duplicação de pares de evoluções
        melhor_evolucao = {}  # Para armazenar a melhor evolução de cada Pokémon

        tasks = []  # Lista de tarefas assíncronas

        # Vamos iterar sobre todos os Pokémons (ajuste conforme necessário)
        for pokemon_id in range(1, 1025):  # Para pegar todos os Pokémon
            tasks.append(processar_dados_pokemon(session, pokemon_id, processados, melhor_evolucao))

        # Aguardar todas as tarefas simultaneamente
        await asyncio.gather(*tasks)

        # Converter os melhores aumentos para uma lista e ordená-los
        aumentos = list(melhor_evolucao.values())
        df_aumentos = pd.DataFrame(aumentos)

        # Ordenar pelos maiores aumentos e pegar os top 7
        maiores_aumentos = df_aumentos.sort_values(by='Aumento', ascending=False).head(7)

        return maiores_aumentos


# Função principal para executar o código
async def pipeline_query5():
    # Passo 1: Calcular aumentos de atributos
    aumentos = await calcular_maiores_aumentos_atributos()

    # Passo 2: Criar o DataFrame a partir dos aumentos
    df_aumentos = pd.DataFrame(aumentos)

    if df_aumentos.empty:
        return "Nenhum dado encontrado para os aumentos."

    # Ordenar os resultados pelos maiores aumentos
    top_7 = df_aumentos.sort_values(
        by=['Aumento', 'Evolução'], ascending=[False, True]
    ).head(7)

    return top_7


# Executar a pipeline e exibir o DataFrame final
df_final = asyncio.run(pipeline_query5())
print(df_final)
