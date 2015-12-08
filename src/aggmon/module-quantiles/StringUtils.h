/**
 * \file StringUtils.h
 *
 * \verbatim
        Revision:       $Revision: 1.0 $
        Revision date:  $Date: 2011/12/28 12:10:48 $
        Committed by:   $Author: Hesse, Guillen $

        This file is part of the PerSyst Monitoring framework

		Copyright (c) 2010-2016, Leibniz-Rechenzentrum (LRZ), Germany
        See the COPYING file in the base directory of the package for details.
   \endverbatim
 */


#ifndef STRINGUTILS_H_
#define STRINGUTILS_H_

#include <string>
#include <set>
#include <iostream>
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <vector>
#include <stdio.h>

/**
 * @brief calculates if all characters in a char * sequence are digits.
 * @param	chrs	[in]	string of characters
 * @return true if all are digits, false otherwise
 *
bool hasOnlyDigits(char * chrs);*/

/**
 * @brief cast integer value to string.
 * @param	val	[in]	integer value
 * @return string of val
 */
std::string int2string(int val);

/**
 * @brief cast integer value to string.
 * @param	val	[in]	long value
 * @return string of val
 */
std::string long2string(long val);

std::string intSet2string(std::set<int> inSet, std::string delimiter);

/*std::vector<char *> getTokensUsingDelimiter(char * buf, char * delim);*/

/**
 * @brief Parses list of chars separated by comma. example: "a03,a05,a07" to a03 a05 a07
 * @param	strlist	[out]	tokens
 * @param	input	[in]	input line
 */
/*void parseStringListWithCommaDelim(std::vector<std::string> & strlist, char * input);*/

/**
 * @brief Parses list of chars separated by comma. example: "3,5,7" to 3 5 and 7
 * @param	intlist	[out]	integer tokens
 * @param	list	[in]	input line
 */
void parseIntListWithCommaDelim(std::vector<int> &intlist, char *list);

/** converts a formatted string to float
 * string is expected in german format, i.e. must have only one comma: 2,34
 * @param	fstring	[in]	formatted string
 * @return	float value
 */
float formattedStringToFloat(char * fstring);

/** Split line
 * Splits a given line from a data file into
 * fields delimited by the default delimiter
 * @param	line	[in]	line to be splitted
 * @param	delimiter	[in]	character to use as delimiter
 * @return	vector of strings with all the tokens
 */
/*std::vector<std::string> splitLine(std::string line, const char delimiter);*/

/*!\class CharCpy
 * \brief class for copying a char string
 */
class CharCpy {
private:
	char * cpy; //!< char copy

public:

	/** Constructor
	 * @param	string to be copied
	 */
	CharCpy(const char * string);

	/** Destructor
	 *
	 */
	~CharCpy();

	/** Copy constructor
	 * @param	other	[in]	object to be copied
	 */
	CharCpy(const CharCpy &other);

	/** Operator equal
	 * @param	other	[in]	object to be copied
	 * @return our object
	 */
	CharCpy &operator=(const CharCpy &other);

	/** Simple getter
	 * @note Dont free this pointer!!!
	 * @return character string copy
	 */
	char * getCpy();
};
#endif /* STRINGUTILS_H_ */
