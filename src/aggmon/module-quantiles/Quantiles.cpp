/*
 * \file Quantiles.cpp
 *
 * \verbatim
        Revision:       $Revision: 1.0 $
        Revision date:  $Date: 2011/12/28 12:10:48 $
        Committed by:   $Author: Hesse $

        This file is part of the PerSyst Monitoring framework

		Copyright (c) 2010-2016, Leibniz-Rechenzentrum (LRZ), Germany
        See the COPYING file in the base directory of the package for details.
   \endverbatim
 */

#include "python2.7/Python.h"
#include "Quantiles.h"
#include <algorithm>

Quantiles::Quantiles() :quantilNumber(NUMBER_QUANTILES), my_mean(0), my_max(0), my_min(0) {
}

Quantiles::Quantiles(const vector<mathType> &valVec,
		unsigned int numOfQuantiles) :
	sortVec(valVec), quantilNumber(numOfQuantiles) {
	sort(sortVec.begin(), sortVec.end());
	//sw_dbgmsg(5, "4. Number of elements in vec = %d\n", sortVec.size());
	updateQuantils();
	//sw_dbgmsg(5, "5. update Quantiles passed\n");
}

Quantiles::~Quantiles() {
}

void Quantiles::updateMinMaxMean() {
	if (sortVec.size() > 1) {
		//sw_dbgmsg(5, "6. case more than one element\n");
		my_min = sortVec.at(0);
		my_max = sortVec.at(sortVec.size() - 1);
		my_mean = mean(sortVec);
	} else if (sortVec.size() == 1) {
		//sw_dbgmsg(5, "7. case only one element\n");
		my_min = sortVec.at(0);
		my_max = my_min;
		my_mean = my_min;
	} else {
		//sw_dbgmsg(5, "8. case only no elements\n");
		my_min = 0;
		my_max = 0;
		my_mean = 0;
	}
}

void Quantiles::updateQuantils() {
	updateMinMaxMean();
	int elementNumber = sortVec.size();
	quantils.resize(quantilNumber + 1); //+min
	mathType factor = elementNumber / (1.0 * quantilNumber);
	quantils[0] = my_min;
	quantils[quantilNumber] = my_max;
	for (unsigned int i = 1; i < quantilNumber; i++) {
		if (elementNumber > 1) {
			int idx = (int) floor(i * factor);
			mathType rest = (i * factor) - idx;
			//sw_dbgmsg(5,"computing quantiles from positions %d and %d\n", idx, idx - 1);
			if(idx == 0){
				quantils[i] = sortVec.at(0);
			} else {
				quantils[i] = sortVec.at(idx - 1)
						+ rest * (sortVec.at(idx) - sortVec.at(idx - 1));
			}
		} else {
			quantils[i] = my_min;
		}
	}
}

void Quantiles::setSortVec(vector<mathType> &inVec) {
	this->sortVec = inVec;
	sort(this->sortVec.begin(), this->sortVec.end());
	updateQuantils();
}

/*void Quantiles::insertElInSortList(mathType val) {
	vector<mathType>::iterator v_it = sortVec.begin();
	while ((v_it != sortVec.end()) && (*v_it < val)) {
		v_it++;
	}
	sortVec.insert(v_it, val); //ok if v_it==sortVec.end(); (insert before)
	updateQuantils();
}*/

/*void Quantiles::insertVecInSortList(vector<mathType> &val) {
	this->sortVec.insert(sortVec.end(), val.begin(), val.end());
	sort(this->sortVec.begin(), this->sortVec.end());
	updateQuantils();
}*/

