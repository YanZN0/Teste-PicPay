import asyncio
import aiohttp
import pandas as pd



async def obter_pokemon_API(session, pokemon_name):
    url = f"https://pokeapi.co/api/v2/pokemon/{pokemon_name}/"
    async with session.get(url) as response:
        if response.status == 200:
            data = await response.json()
            stats = {
                stat['stat']['name']: stat['base_stat']
                for stat in data['stats']
                if stat['stat']['name'] in ['hp', 'attack', 'defense', 'speed']
            }
            return {'name': data['name'], 'stats': stats}
        return None


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


        if (pre_evo, evo) in processados:
            continue

        pokemon_atual = await obter_pokemon_API(session, pre_evo)
        proxima_evolucao = await obter_pokemon_API(session, evo)

        if not pokemon_atual or not proxima_evolucao:
            continue

    
        for atributo, valor_base in pokemon_atual['stats'].items():
            aumento = proxima_evolucao['stats'].get(atributo, 0) - valor_base
            if aumento > 0:
    
                if pre_evo not in melhor_evolucao or aumento > melhor_evolucao[pre_evo]['Aumento']:
                    melhor_evolucao[pre_evo] = {
                        'Pré-evolução': pre_evo,
                        'Evolução': evo,
                        'Atributo': atributo,
                        'Aumento': aumento
                    }

        processados.add((pre_evo, evo))



async def calcular_maiores_aumentos_atributos():
    async with aiohttp.ClientSession() as session:
        aumentos = []
        processados = set()  
        melhor_evolucao = {}  

        tasks = [] 


        for pokemon_id in range(1, 1025):  
            tasks.append(processar_dados_pokemon(session, pokemon_id, processados, melhor_evolucao))

        await asyncio.gather(*tasks)


        aumentos = list(melhor_evolucao.values())
        df_aumentos = pd.DataFrame(aumentos)


        maiores_aumentos = df_aumentos.sort_values(by='Aumento', ascending=False).head(7)

        return maiores_aumentos



async def pipeline_query5():

    aumentos = await calcular_maiores_aumentos_atributos()


    df_aumentos = pd.DataFrame(aumentos)

    if df_aumentos.empty:
        return "Nenhum dado encontrado para os aumentos."


    top_7 = df_aumentos.sort_values(
        by=['Aumento', 'Evolução'], ascending=[False, True]
    ).head(7)

    return top_7
