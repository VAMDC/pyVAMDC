**README pyVAMDC version 0.1**

pyVAMDC is a Python library to use, extract and manipulate atomic and molecular data extracted from the VAMDC infrastructure. 

***Installing the library***
Open a terminal and go the pyVAMDC directory containing this readme file. 
Then run the command: 
```python setup.py install ```

***Command line interface***
The project also exposes a `vamdc` command that reuses the library features without writing extra code. When developing locally you can invoke it in-place with uv:
```bash
uvx --from . vamdc get nodes
uvx --from . vamdc get species
uvx --from . vamdc count lines --inchikey=UFHFLCQGNIYNRP-UHFFFAOYSA-N --node=vald
```
Cached CSV files are stored under `~/.cache/vamdc` by default; use `--cache-dir` or `--force-refresh` when you need to override the defaults.

**Using Examples**
Have a look at the code in the ```tests``` subfloder.
