import bz2
import csv
import io
import itertools
import logging
import re
import xml.etree.ElementTree as etree
from urllib.parse import quote

import mwparserfromhell


logger = logging.getLogger(__name__)


def _extract_index(filepath):
    with open(filepath, "rb") as compressed_file:
        binary_file = bz2.BZ2File(filename=compressed_file)
        text_file = io.TextIOWrapper(binary_file, encoding="utf-8", newline="")
        return sorted({int(row[0]) for row in csv.reader(text_file, delimiter=":")})


def _pairwise(index):
    starts, ends = itertools.tee(index)
    _ = next(ends, None)
    return itertools.zip_longest(starts, ends)


def _extract_content(filepath, start, end):
    """Extract articles from a single stream of a multistream WikiMedia XML file."""
    logger.info("generating examples from = %s", filepath)
    with open(filepath, "rb") as compressed_file:
        compressed_file.seek(start)
        compressed_data = compressed_file.read(end - start if end else -1)
        binary_data = bz2.BZ2Decompressor().decompress(compressed_data)
        # Enclose within a single root node to avoid ParseError: junk after document element
        binary_data = b"<mediawiki>" + binary_data + b"</mediawiki>"
        with io.StringIO(binary_data.decode(encoding="utf-8")) as text_stream:
            for _, elem in etree.iterparse(text_stream):
                if not elem.tag.endswith("page"):
                    continue
                namespace = elem.tag[:-4]
                ns = elem.find(f"./{namespace}ns").text
                redirect = elem.find(f"./{namespace}redirect")
                # Filter pages that are not in the "main" namespace or that are redirects
                if ns != "0" or redirect is not None:
                    elem.clear()
                    continue
                id_ = elem.find(f"./{namespace}id").text
                title = elem.find(f"./{namespace}title").text
                raw_content = elem.find(f"./{namespace}revision/{namespace}text").text
                elem.clear()
                # Filter empty pages
                if raw_content is None:
                    continue
                yield id_, title, raw_content


def _clean_content(inputs, language, min_text_length=0):
    """Clean raw wikicode to extract text."""
    id_, title, raw_content = inputs
    try:
        text = _parse_and_clean_wikicode(raw_content, parser=mwparserfromhell, language=language)
    except mwparserfromhell.parser.ParserError as e:
        logger.error("mwparserfromhell ParseError: %s", e)
        return
    if not text or len(text) < min_text_length:
        return
    url = _construct_url(title, language)
    yield id_, {"id": id_, "url": url, "title": title, "text": text}


def _parse_and_clean_wikicode(raw_content, parser, language):
    """Strip formatting and unwanted sections from raw page content."""
    wikicode = parser.parse(raw_content)

    # Filters for magic words that are parser instructions -- e.g., __NOTOC__
    re_rm_magic = re.compile("__[A-Z]*__", flags=re.UNICODE)

    # Filters for file/image links.
    media_prefixes = "|".join(["File", "Image", "Media"] + MEDIA_ALIASES.get(language, []))
    re_rm_wikilink = re.compile(f"^(?:{media_prefixes}):", flags=re.IGNORECASE | re.UNICODE)

    def rm_wikilink(obj):
        return bool(re_rm_wikilink.match(str(obj.title)))

    # Filters for references and tables
    def rm_tag(obj):
        return str(obj.tag) in {"ref", "table"}

    # Leave category links in-place but remove the category prefixes
    cat_prefixes = "|".join(["Category"] + CAT_ALIASES.get(language, []))
    re_clean_wikilink = re.compile(f"^(?:{cat_prefixes}):", flags=re.IGNORECASE | re.UNICODE)

    def is_category(obj):
        return bool(re_clean_wikilink.match(str(obj.title)))

    def clean_wikilink(obj):
        text = obj.__strip__()
        text = re.sub(re_clean_wikilink, "", text)
        obj.text = text

    def try_replace_obj(obj):
        try:
            clean_wikilink(obj)
        except ValueError:
            # For unknown reasons, objects are sometimes not found.
            pass

    def try_remove_obj(obj, section):
        try:
            section.remove(obj)
        except ValueError:
            # For unknown reasons, objects are sometimes not found.
            pass

    section_text = []
    # Filter individual sections to clean.
    for section in wikicode.get_sections(flat=True, include_lead=True, include_headings=True):
        for obj in section.ifilter_wikilinks(recursive=True):
            if rm_wikilink(obj):
                try_remove_obj(obj, section)
            elif is_category(obj):
                try_replace_obj(obj)
        for obj in section.ifilter_tags(matches=rm_tag, recursive=True):
            try_remove_obj(obj, section)

        section_text.append(re.sub(re_rm_magic, "", section.strip_code().strip()))
    return "\n\n".join(section_text)


def _construct_url(title, language):
    # See: https://meta.wikimedia.org/wiki/Help:URL
    return f"https://{language}.wikisource.org/wiki/{quote(title)}"
