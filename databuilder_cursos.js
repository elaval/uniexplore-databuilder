/* jshint undef: true, unused: true */
/* global require, _, console */
"use strict";

var _ = require("underscore")._;
var d3 = require("d3");
var fs  = require("fs");
var Q = require("q");

var filename = "./inputData/estudiantes_x_curso.txt";
var outdir="./data/cursos/";
var cursosfilename = "./data/cursos.txt";

//var filename = "./dummy.txt";
var categoryAttribute = "sigla";
var xAttribute = "";
var yAttribute = "";
var data = [];

var output_properties = ["id", "semestre", "genero", "sigla",  "unidad_academ_curso", "carrera", "unidad","area", "area_oecd", "ano_ingreso","agnosEnCarrera", "prom_paa", "dependencia", "nota_final", "actividades_sakai", "actividades_aleph", "actividades_ezproxy"];

var notify = function(msg) {
	console.log(msg);
};

var processor = {};

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

		fs.writeFileSync(outdir+filename, outdata);

		deferred.resolve(mydata.length);

	});

	return deferred.promise;
};


processor.recSave = function(categories,categoriesObj) {
	if (categories.length >0) {
		var category = categories[0];
		notify("Saving: "+category+" - "+categoriesObj[category].length+" records");
		processor.save(category+".txt", categoriesObj[category]).then(function(num) {
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
		outdata += i == 0 ? field : "\t"+field;
	});
	outdata += "\n";

	_.each(data, function(d) {

		_.each(fields, function(field, i) {
			outdata += i == 0 ? d[field] : "\t"+d[field];
		});
		outdata += "\n";
	})
	fs.writeFileSync(cursosfilename, outdata);

}

processor.startupFilter = function(data) {
	var filteredData = _.filter(data, function(d) {
		return (d.ano==2012 && d.periodo == 21) || (d.ano==2012 && d.periodo == 22) || (d.ano==2013 && d.periodo == 21); 
	})
		
	return filteredData;
};

processor.dataSetup = function(data) {
	var idnum = 0;
	var idDict = {};

	_.each(data, function(d) {
		if (!idDict[d.nro_alumno]) {
			idnum++;
			idDict[d.nro_alumno] = idnum;
		} 
		d.id =idDict[d.nro_alumno];

		if (d.ano == 2012 && d.periodo == 21) {
			d.semestre = "2012 (I)";
		} else if (d.ano == 2012 && d.periodo == 22) {
			d.semestre = "2012 (II)";
		} else if (d.ano == 2013 && d.periodo == 21) {
			d.semestre = "2013 (I)";
		} else {
			d.semestre = "otro";
		} 

		if (d.estabecimiento_tipo == "MUNICIPAL") {
			d.dependencia = "Municipal";
		} else if (d.estabecimiento_tipo == "PARTICULAR" && d.establecimiento_finaciamiento == "PAGADO") {
			d.dependencia = "Particular Pagado";
		} else if (d.estabecimiento_tipo == "PARTICULAR") {
			d.dependencia = "Particular Subvencionado";
		} else {
			d.dependencia = "S/I";
		}

		d.agnosEnCarrera = d.ano-d.ano_ingreso+1;


	});

	notify(idnum);
		
	return data;
};


var regression = function(data) {
  // Calculates linear regresion on a set of data points: [[x1,y1], [x2,y2], ... [xn,yn]]
  // Returns object {slope: slope, intercept:intercept, r2: r2}
    var sumX = 0;
    var sumY = 0;
    var sumX2 = 0;
    var sumY2 = 0;
    var sumXY = 0;
    var n = data.length;

    _.each(data, function(d) {
      sumX += d[0];
      sumY += d[1];
      sumX2 += d[0]*d[0];
      sumY2 += d[1]*d[1];
      sumXY += d[0]*d[1];
    });

    var slope = (n*sumXY-sumX*sumY)/(n*sumX2-sumX*sumX);
    var intercept = (sumY-slope*sumX)/n;
    var r2 = Math.pow((n*sumXY-sumX*sumY)/Math.sqrt((n*sumX2-sumX*sumX)*(n*sumY2-sumY*sumY)),2);
    var points = [];

    _.each(data, function(d) {
      var x = d[0];
      var y = d[0]*slope + intercept;
      points.push([x,y]);
    });


    return {"slope":slope, "intercept":intercept, "r2": r2, "points":points};

};

notify("Loading: "+filename);

processor.calculateRegression = function(categories, categoriesObj) {
	var outdata = [];
	_.each(categories, function(curso) {
		notify(curso);

		var groupByUnidad = _.groupBy(categoriesObj[curso], function(d) {return d.unidad_academ_curso});
		var unidades = _.keys(groupByUnidad);
		if (unidades.length != 1) {notify("Error - "+unidades.length +" unidades")}

		var unidad = unidades[0];
		
		var groupBySemester = _.groupBy(categoriesObj[curso], function(d) {return d.ano+" "+(d.periodo == 21 ? "(I)" : d.periodo == 22 ? "(II)" : d.periodo == 23 ? "(III)" : "(-)" )})
		
		_.each(_.keys(groupBySemester), function(semestre) {
			//notify(curso+" - "+semestre)

			var datapoints = _.map(groupBySemester[semestre], function(d) {
				//notify([+d.actividades_sakai, +d.nota_final]);
				return [+d.actividades_sakai, +d.nota_final];
			})
			var regdata = regression(datapoints);

			var sumaSakai = _.reduce(datapoints, function(memo,d) {return +d[0]+memo}, 0);
			var promedioSakai = sumaSakai/datapoints.length;
			var sumaNotas = _.reduce(datapoints, function(memo,d) {return +d[1]+memo}, 0);
			var promedioNotas = sumaNotas/datapoints.length;
			var outRecord = {};

			outRecord.sigla = curso;
			outRecord.semestre = semestre;
			outRecord.unidad = unidad;
			outRecord.n = datapoints.length;
			outRecord.promedio_sakai = promedioSakai;
			outRecord.promedio_notas = promedioNotas;
			outRecord.r2 = regdata.r2;
			outRecord.slope = regdata.slope;
			outRecord.intercept = regdata.intercept;

			if (outRecord.r2 >=0) {
				outdata.push(outRecord);
			}
		})



	})
	return outdata;
}

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

		// get data from each course
		var cursosData = processor.calculateRegression(categories,categoriesObj);
		notify("cursosData: "+cursosData.length)

		processor.cursosSave(cursosData, ['sigla', 'semestre',  'unidad', 'n', 'promedio_sakai', 'promedio_notas', 'r2', 'slope', 'intercept']);



		processor.recSave(categories,categoriesObj);
	  	notify("End");
	});

