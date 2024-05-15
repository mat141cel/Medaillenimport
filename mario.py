from reusable_code import *
import asyncio
import re
import argparse


async def fetch_url(url, record_list):
    response_text = make_request(url)
    root = ET.fromstring(response_text)
    # Define namespaces
    l = '{http://www.lido-schema.org}'
    for record in root.findall(".//{http://www.openarchives.org/OAI/2.0/}record"):
        handle_id = extr_text(record, f".//{l}objectPublishedID[2]")

        for event in record.findall(f".//{l}event"):
            event_type = extr_text(event, f"{l}eventType/{l}term[2]")
            if event_type == "Fund": fundort = extr_text(event, f"{l}eventPlace/{l}displayPlace")

        
        # funktioniert, wer weiß wieso 
        test = record.findall(f".//{l}object/{l}objectWebResource")
        nk_fundkomplex = ' | '.join([elem.text for elem in test if re.search(r'fundkomplex', elem.text)])
        ocre_crro_verknüpfung = ' | '.join([elem.text for elem in test if re.search(r'numismatics', elem.text)])
        #print(nk_fundkomplex, ocre_crro_verknüpfung)


        # literatur ähnlich blöd wie fundkomplex
        literaturfeld = record.findall(f".//{l}object/{l}objectNote")
        nk_fundkomplex = ' | '.join([elem.text for elem in literaturfeld])

        record_list.append([handle_id, fundort, nk_fundkomplex, literaturfeld, ocre_crro_verknüpfung])



async def main():
    # Opening JSON file
    f = open('mario.json.json')
    url_list = json.load(f)[:10]
    print(F"number of medals to scrape: {len(url_list)}")
    # Set the desired rate of requests per second
    results = []
    async def process_url(url):
        await fetch_url(f"https://www.kenom.de/oai/?verb=GetRecord&metadataPrefix=lido&identifier={url}", results)
    tasks = [process_url(url) for url in url_list]
    await asyncio.gather(*tasks)
    print(results)
    save_json("output_mario", results)

asyncio.run(main())

async def process_oai():
    url_list = get_oai_urls("metadataPrefix=oai_dc&set=relation:fundkomplex:true")
    save_json("mario.json", url_list)


