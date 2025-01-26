
import aiohttp
import pandas as pd

async def obter_pokemons_da_API():
    async with aiohttp.ClientSession() as session:
        pokemons = []
        url = "https://pokeapi.co/api/v2/pokemon/"
        
        while url:
            async with session.get(url) as response:
                data = await response.json()
                pokemons.extend(data["results"])
                url = data['next']

        pokemons_detalhados = []
        for pokemon in pokemons:
            async with session.get(pokemon['url']) as response:
                pokemon_data = await response.json()
                if pokemon_data['is_default']:
                    nome = pokemon_data['name']
                    altura = pokemon_data['height']
                    pokemons_detalhados.append({'name': nome, 'height': altura})

        return pd.DataFrame(pokemons_detalhados)

async def obter_pokémon_mais_alto(df = pd.DataFrame) -> pd.DataFrame:
    
    dataframe = df.sort_values(by=['height'], ascending=False).head(1)


    return dataframe




async def pipeline_pokemon_mais_alto_primeira_forma():
    resultado = await obter_pokemons_da_API()
    resultado2 =  await obter_pokémon_mais_alto(resultado)
    return resultado2
