


import aiohttp
import asyncio
import pandas as pd


async def fetch_pokemon_urls(session):
    """Obtém as URLs de todos os Pokémon na PokeAPI."""
    url = "https://pokeapi.co/api/v2/pokemon?limit=1000"
    async with session.get(url) as response:
        data = await response.json()
        return [pokemon['url'] for pokemon in data['results']]


async def fetch_pokemon_info(url, session):
    """Obtém as informações detalhadas de um Pokémon pela URL."""
    async with session.get(url) as response:
        return await response.json()


async def golpe_mais_aprendido_na_forma_padrao():
    """Identifica o golpe mais aprendido entre os Pokémon na forma padrão."""
    async with aiohttp.ClientSession() as session:
        # Obter URLs de todos os Pokémon
        pokemon_urls = await fetch_pokemon_urls(session)
        
        # Reunir dados de Pokémon em forma padrão
        tasks = [fetch_pokemon_info(url, session) for url in pokemon_urls]
        pokemons_data = await asyncio.gather(*tasks)

        golpes_por_versao = {}
        for pokemon_data in pokemons_data:
            # Considera apenas Pokémon na forma padrão
            if not pokemon_data['is_default']:
                continue

            for move in pokemon_data['moves']:
                for version_detail in move['version_group_details']:
                    if version_detail['move_learn_method']['name'] == 'level-up':
                        golpe = move['move']['name']
                        versao = version_detail['version_group']['name']

                        # Contabiliza aprendizados por golpe e versão
                        if golpe not in golpes_por_versao:
                            golpes_por_versao[golpe] = {}
                        golpes_por_versao[golpe][versao] = golpes_por_versao[golpe].get(versao, 0) + 1

        # Construir DataFrame com golpes e frequências
        dados = [
            {"Golpe": golpe, "Versão": versao, "Frequência": frequencia}
            for golpe, versoes in golpes_por_versao.items()
            for versao, frequencia in versoes.items()
        ]
        df = pd.DataFrame(dados)

        # Identificar golpe mais aprendido e a versão mais frequente
        golpe_mais_aprendido = df.groupby("Golpe")["Frequência"].sum().idxmax()
        total_aprendizagens = df.groupby("Golpe")["Frequência"].sum().max()
        versao_mais_frequente = (
            df[df["Golpe"] == golpe_mais_aprendido]
            .sort_values(by="Frequência", ascending=False)
            .iloc[0]["Versão"]
        )
        frequencia_versao = (
            df[df["Golpe"] == golpe_mais_aprendido]
            .sort_values(by="Frequência", ascending=False)
            .iloc[0]["Frequência"]
        )

        mensagem = (
            f"O golpe mais aprendido na forma padrão é '{golpe_mais_aprendido}', aprendido "
            f"{total_aprendizagens} vezes.\n"
            f"A versão onde isso ocorre com mais frequência é '{versao_mais_frequente}', "
            f"com {frequencia_versao} aprendizados."
        )
        print(mensagem)

        # Retornando o DataFrame como parte do resultado
        return {
            "mensagem": mensagem,
            "dataframe": df,
            "golpe_mais_aprendido": golpe_mais_aprendido
        }

async def pokemon_com_mais_attack_no_golpe_mais_aprendido(golpe_mais_aprendido):
    """Identifica o Pokémon com o maior valor de 'attack' e os 10 melhores entre os que aprendem o golpe mais aprendido."""
    async with aiohttp.ClientSession() as session:
        # Obter URLs de todos os Pokémon
        pokemon_urls = await fetch_pokemon_urls(session)

        # Reunir dados de Pokémon
        tasks = [fetch_pokemon_info(url, session) for url in pokemon_urls]
        pokemons_data = await asyncio.gather(*tasks)

        # Filtrar Pokémon que aprendem o golpe mais aprendido
        pokemons_que_aprendem_o_golpe = []
        for pokemon_data in pokemons_data:
            if not pokemon_data['is_default']:
                continue

            for move in pokemon_data['moves']:
                for version_detail in move['version_group_details']:
                    if version_detail['move_learn_method']['name'] == 'level-up' and move['move']['name'] == golpe_mais_aprendido:
                        pokemons_que_aprendem_o_golpe.append({
                            "nome": pokemon_data['name'],
                            "attack": pokemon_data['stats'][1]['base_stat'],  # Attack é geralmente o índice 1
                            "versao": version_detail['version_group']['name']
                        })

        # Criar DataFrame com os Pokémon que aprendem o golpe
        df_pokemon = pd.DataFrame(pokemons_que_aprendem_o_golpe)

        # Encontrar o Pokémon com maior 'attack'
        if not df_pokemon.empty:
            pokemon_top_attack = df_pokemon.loc[df_pokemon['attack'].idxmax()]
            mensagem_top_attack = (
                f"O Pokémon com o maior 'attack' que aprende '{golpe_mais_aprendido}' é {pokemon_top_attack['nome']} "
                f"com {pokemon_top_attack['attack']} de ataque. Isso ocorre na versão '{pokemon_top_attack['versao']}'."
            )

            # Top 10 Pokémon com maior ataque
            df_top_10 = df_pokemon.sort_values(by="attack", ascending=False).head(10)

            # Mensagem adicional para exibição do Top 10
            mensagem_top_10 = "\nTop 10 Pokémon que aprendem o golpe mais aprendido:\n" + df_top_10.to_string(index=False)
        else:
            mensagem_top_attack = f"Nenhum Pokémon encontrado que aprende o golpe '{golpe_mais_aprendido}'."
            mensagem_top_10 = "Nenhum dado disponível para o Top 10 Pokémon."

        # Exibir resultados
        print(mensagem_top_attack)
        print(mensagem_top_10)

        return {
            "mensagem_attack": mensagem_top_attack,
            "df_top_10": df_top_10 if not df_pokemon.empty else pd.DataFrame(),
        }

async def pipeline():
    """Pipeline principal para análise dos Pokémon."""
    print("Iniciando pipeline de análise Pokémon...\n")

    # Etapa 1: Identifica o golpe mais aprendido na forma padrão
    resultado_golpe = await golpe_mais_aprendido_na_forma_padrao()

    # Etapa 2: Identifica o Pokémon com maior ataque e o Top 10
    resultado_top_10 = await pokemon_com_mais_attack_no_golpe_mais_aprendido(resultado_golpe["golpe_mais_aprendido"])

    print("\nPipeline concluída com sucesso!")

    # Exibe os resultados detalhados no console
    print("\nDetalhes do Pokémon com maior ataque:")
    print(resultado_top_10["mensagem_attack"])

    if not resultado_top_10["df_top_10"].empty:
        print("\nTop 10 Pokémon que aprendem o golpe mais aprendido:")
        print(resultado_top_10["df_top_10"])
    else:
        print("\nNenhum dado disponível para o Top 10 Pokémon.")

    # Retorna todos os resultados
    return {
        "golpe_mais_aprendido": resultado_golpe["golpe_mais_aprendido"],
        "mensagem_golpe": resultado_golpe["mensagem"],
        "mensagem_attack": resultado_top_10["mensagem_attack"],
        "top_10_dataframe": resultado_top_10["df_top_10"],
    }


if __name__ == "__main__":
    asyncio.run(pipeline())
