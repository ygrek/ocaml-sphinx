
GEN=index.html
WEB=sphinx.forge.ocamlcore.org:/home/groups/sphinx/htdocs

html: $(GEN)

%.html: %.brtx
	brtx -html -doc -i $< -o $@

upload: index.html
	rsync -avhz $< $(WEB)/
	make -C .. doc
	rsync -avihz ../_build/api.docdir/ $(WEB)/api

clean:
	rm -f $(GEN)

