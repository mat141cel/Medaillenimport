from reusable_code import *
import asyncio
import re
import argparse
from tqdm import tqdm

def fetch_url(url):
    r = requests.get(url)
    r.encoding = 'utf-8'
    response_text = r.text
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
        nk_fundkomplex = ' | '.join([elem.text for elem in test if re.search(r'fundkomplex', elem.text)]).replace("https://kenom.gbv.de/fundkomplex/", "http://hdl.handle.net/428894.vzg/")
        ocre_crro_verknüpfung = ' | '.join([elem.text for elem in test if re.search(r'numismatics', elem.text)])
      	
        # literatur ähnlich blöd wie fundkomplex
        text_content = [elem.text for elem in record.findall(f".//{l}object/{l}objectNote")]
        literaturfeld = []
        for i, text in enumerate(text_content): 
            if text == "Literatur": literaturfeld.append(text_content[i-1])
        literaturfeld = " | ".join(literaturfeld)

        dat_begin = dat_ende = dat_verbal = None # initialize the variables
        for event in record.findall(f".//{l}event"):
            event_type = extr_text(event, f"{l}eventType/{l}term[2]")
            if event_type == "Herstellung":
                dat_begin = extr_text(event, f"{l}eventDate/{l}date/{l}earliestDate")
                dat_ende = extr_text(event, f"{l}eventDate/{l}date/{l}latestDate")
        try:
            if "-" in dat_begin: dat_begin = dat_begin[:4]
            if "-" in dat_ende: dat_ende = dat_ende[:4]
            if int(dat_begin) < 750 or int(dat_ende) < 750:
                records = {"handle_id": handle_id,
                        "fundort": fundort,
                        "nk_fundkomplex": nk_fundkomplex,
                        "literaturfeld": literaturfeld,
                        "ocre_crro_verknüpfung": ocre_crro_verknüpfung}
                print(records)
                return records
        except: pass
        


def main():
    # Opening JSON file
    with open('mario.json.json') as f: url_list = json.load(f)[:100]
    batch_size = 1000
    total_batches = (len(url_list) + batch_size - 1) // batch_size

    record_list = []  # List to store all fetched records
    for i in range(0, len(url_list), batch_size):
        batch = url_list[i : i + batch_size]
        for _, url in tqdm(enumerate(batch), total=len(batch), desc=f"Fetching batch {i//batch_size + 1}"):
            if r := fetch_url(f"https://www.kenom.de/oai/?verb=GetRecord&metadataPrefix=lido&identifier={url}"):
                record_list.append(r)
        print(f"Batch {i//batch_size + 1} completed. Total records fetched: {len(record_list)}")

    # Optional: Save the results after each batch for extra safety
    # Save the results after each batch for extra safety
    print(record_list)
    with open(f"mario_output.json", "w", encoding='utf-8') as f:
        json.dump(record_list, f, ensure_ascii=False)

main()

async def process_oai():
    url_list = get_oai_urls("metadataPrefix=oai_dc&set=relation:fundkomplex:true")
    save_json("mario.json", url_list)


