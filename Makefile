
.PHONY: test build byte native clean doc

build: byte native
byte: sphinx.cma
native: sphinx.cmxa
test: test.native

DEPS=-package extlib,deriving.syntax,bitstring.syntax -syntax camlp4o

sphinx.cma: sphinx.ml
	ocamlfind ocamlc -a -annot -g $(DEPS) $^ -o $@

sphinx.cmxa: sphinx.ml
	ocamlfind ocamlopt -a -annot -g $(DEPS) $^ -o $@

test.native: sphinx.cmxa test.ml
	ocamlfind ocamlopt -verbose -linkpkg -annot -g $(DEPS) $^ -o $@

doc:
	-mkdir doc
	ocamlfind ocamldoc -html -v $(DEPS) sphinx.ml -d doc

install:
	ocamlfind install sphinx META $(wildcard sphinx.cmx sphinx.cmxa sphinx.a sphinx.lib sphinx.cma sphinx.cmi sphinx.mli)

uninstall:
	ocamlfind remove sphinx

clean:
	rm -f *.cm* *.annot *.native *.o *.obj *.lib *.a

