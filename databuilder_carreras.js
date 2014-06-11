/* jshint undef: true, unused: true */
/* global require, _, console */
"use strict";

var _ = require("underscore")._;
var d3 = require("d3");
var fs  = require("fs");
var Q = require("q");

var indir = "./inputData";
var filename = "./inputData/estudiantes_x_carrera.txt";
var outdir="./data/carreras/";
var cursosfilename = "./data/carreras.txt";

//var filename = "./dummy.txt";
var categoryAttribute = "carrera";
var data = [];

//var output_properties = ["id", "semestre", "genero", "sigla",  "unidad_academ_curso", "carrera", "unidad", "ano_ingreso", "prom_paa", "dependencia", "nota_final", "actividades_sakai", "actividades_aleph", "actividades_ezproxy"];

//var output_properties = ["id","semestre","genero","unidad","carrera","area", "area_oecd","ano_ingreso", "agnosEnCarrera","prom_paa","dependencia","promedio_notas","actividades_sakai","actividades_aleph","actividades_ezproxy"];
var output_properties = [
	"id",
	"semestre",
	"genero",
	"unidad",
	"carrera",
	"area",
	"area_oecd",
	"ano_ingreso",
	"agnosEnCarrera",
	"prom_paa",
	"dependencia",
	"promedio_notas",
	"actividades_sakai",
	"actividades_aleph",
	"actividades_ezproxy",

	"actividades_aleph_impreso",
	"actividades_aleph_digital",
	"actividades_aleph_espacio",

	"actividades_sakai_test",
	"actividades_sakai_contenido",
	"actividades_sakai_leer",
	"actividades_sakai_escribir",
	"actividades_sakai_personal",
	"actividades_sakai_informacion"
	];

var notify = function(msg) {
	console.log(msg);
};

var processor = {};


var escape_filename = function(filename) {
	var newfilename = filename.replace(/\//g, "_");
	return newfilename;
};

processor.load = function(filename) {
	var deferred = Q.defer();

	fs.readFile(filename, "utf8", function (err, tsvdata) {
		if (err) {
			deferred.reject(new Error(err));
		} else {
			var data = d3.tsv.parse(tsvdata);
			deferred.resolve(data);
		}
	});
		
	return deferred.promise;
};

processor.save = function(filename, mydata) {
	var deferred = Q.defer();

	fs.mkdir(outdir, function() {

		var outdata = "";
		_.each(output_properties, function(key, i) {
			outdata += i===0 ? key : "\t"+key;
		});
		outdata += "\n";
		_.each(mydata, function(d) {
			_.each(output_properties, function(key, j) {
				outdata += j===0 ? d[key] : "\t"+d[key];
			});
			outdata += "\n";
		});

		fs.writeFileSync(outdir+escape_filename(filename), outdata);

		deferred.resolve(mydata.length);

	});

	return deferred.promise;
};


processor.recSave = function(categories,categoriesObj) {
	if (categories.length >0) {
		var category = categories[0];
		notify("Saving: "+category+" - "+categoriesObj[category].length+" records");
		processor.save(category+".txt", categoriesObj[category]).then(function() {
			processor.recSave(_.last(categories, categories.length-1),categoriesObj);
		})
		.catch(function (error) {
			notify("Error: "+category + " - " + error);
			processor.recSave(_.last(categories, categories.length-1),categoriesObj);
		});
    

	}
};

processor.cursosSave = function(data, fields) {
	var outdata = "";
	_.each(fields, function(field, i) {
		outdata += i === 0 ? field : "\t"+field;
	});
	outdata += "\n";

	_.each(data, function(d) {

		_.each(fields, function(field, i) {
			outdata += i === 0 ? d[field] : "\t"+d[field];
		});
		outdata += "\n";
	});
	fs.writeFileSync(cursosfilename, outdata);

};

processor.startupFilter = function(data) {
	var filteredData = _.filter(data, function(d) {
		return (d.ano==2012 && d.periodo == 21) || (d.ano==2012 && d.periodo == 22) || (d.ano==2013 && d.periodo == 21); 
	});
		
	return filteredData;
};

processor.dataSetup = function(data) {
	var idnum = 0;
	var idDict = {};

	_.each(data, function(d) {
		if (!idDict[d.rut]) {
			idnum++;
			idDict[d.rut] = idnum;
		} 
		d.id =idDict[d.rut];

		if (d.ano == 2012 && d.periodo == 21) {
			d.semestre = "2012 (I)";
		} else if (d.ano == 2012 && d.periodo == 22) {
			d.semestre = "2012 (II)";
		} else if (d.ano == 2013 && d.periodo == 21) {
			d.semestre = "2013 (I)";
		} else {
			d.semestre = "otro";
		} 

		if (d.establecimiento_tipo == "MUNICIPAL") {
			d.dependencia = "Municipal";
		} else if (d.establecimiento_tipo == "PARTICULAR" && d.establecimiento_financiamiento == "PAGADO") {
			d.dependencia = "Particular Pagado";
		} else if (d.establecimiento_tipo == "PARTICULAR") {
			d.dependencia = "Particular Subvencionado";
		} else {
			d.dependencia = "S/I";
		}

		d.actividades_sakai = d.actividades_sakai_total;
		d.actividades_aleph = d.total_actividades_aleph;

		d.actividades_aleph_impreso = d.impreso_actividades_aleph;
		d.actividades_aleph_digital = d.digital_actividades_aleph
		d.actividades_aleph_espacio = d.espacio_actividades_aleph

		d.agnosEnCarrera = d.ano-d.ano_ingreso+1;


	});

	notify(idnum);
		
	return data;
};

var saveCarreras = function(carreras, carrerasObj) {

	var deferred = Q.defer();

	var outdata = "carrera\tarea\tn\tfilename\n";

	_.each(carreras, function(d) {
		var filename = escape_filename(d);
		var n = carrerasObj[d].length;
		var area = carrerasObj[d][0].area;
		outdata += d+"\t"+area+"\t"+n+"\t"+filename+".txt\n";
	})

	fs.writeFileSync(cursosfilename, outdata);

	deferred.resolve();


	return deferred.promise;
}



notify("Loading: "+filename);


processor.load(filename)
	.then(function(loadedData) {
		data = loadedData;
		notify("Length prefilter: "+data.length);
		data = processor.startupFilter(data);
		data = processor.dataSetup(data);
		notify("Length postfilter: "+data.length);

		notify("Identifying categories");
		var categoriesObj = _.groupBy(data, function(d) {return d[categoryAttribute];});
	  	var categories = _.keys(categoriesObj).sort();
		notify("Found "+categories.length+" categories");

		processor.recSave(categories,categoriesObj);

		saveCarreras(categories,categoriesObj);
	  	notify("End");
	});

