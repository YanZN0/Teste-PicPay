import aiohttp
import asyncio
import pandas as pd

async def obter_pokemons() -> list:
    url = "https://pokeapi.co/api/v2/pokemon/"
    pokemons = []

    async with aiohttp.ClientSession() as session:
        while url:
            async with session.get(url) as response:
                data = await response.json()
                pokemons.extend(data["results"])
                url = data['next']

    return pokemons

async def obter_dados_pokemon(pokemon_url: str, session) -> dict:
    async with session.get(pokemon_url) as response:
        return await response.json()

async def obter_cadeia_evolutiva(species_url: str, session) -> dict:
    async with session.get(species_url) as response:
        species_data = await response.json()
        evolution_chain_url = species_data.get('evolution_chain', {}).get('url', '')

    if evolution_chain_url:
        async with session.get(evolution_chain_url) as evolution_response:
            return await evolution_response.json()
    return {}

async def processar_evolucoes(pokemons: list) -> pd.DataFrame:
    pokemons_com_evolucoes = []

    async with aiohttp.ClientSession() as session:
        for item in pokemons:
            pokemon_url = item['url']
            pokemon_data = await obter_dados_pokemon(pokemon_url, session)

            species_url = pokemon_data.get('species', {}).get('url', '')
            if not species_url:
                continue

            evolution_data = await obter_cadeia_evolutiva(species_url, session)
            chain = evolution_data.get('chain', {})

            if not chain:
                continue

            pokemon_atual = chain['species']['name']
            evolucoes = []

            next_evolution = chain.get('evolves_to', [])
            while next_evolution:
                evolucao = next_evolution[0]['species']['name']
                evolucoes.append(evolucao)
                next_evolution = next_evolution[0].get('evolves_to', [])

            pokemons_com_evolucoes.append({
                'pokemon': pokemon_atual,
                'evolutions': evolucoes,
                'num_caminhos': len(evolucoes)
            })

    return pd.DataFrame(pokemons_com_evolucoes)

async def exibir_pokemons_com_evolution() -> pd.DataFrame:
    pokemons = await obter_pokemons()
    return await processar_evolucoes(pokemons)

def contar_pokemons_com_multiplos_caminhos(df: pd.DataFrame) -> int:
    return df[df['num_caminhos'] > 1].shape[0]

def pipeline_pokemon_com_multiplos_caminhos():
    loop = asyncio.get_event_loop()
    df_pokemons = loop.run_until_complete(exibir_pokemons_com_evolution())

    # Filtragem e contagem com pandas diretamente
    quantidade = contar_pokemons_com_multiplos_caminhos(df_pokemons)
    print(f"O número de Pokémon com mais de um caminho evolutivo é: {quantidade}")
