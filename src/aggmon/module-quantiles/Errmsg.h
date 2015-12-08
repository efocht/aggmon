/*
 * \file Errmsg.h
 * \verbatim
        Revision:       $Revision: 1.0 $
        Revision date:  $Date: 2011/12/28 12:10:48 $
        Committed by:   $Author: Hesse, Guillen $

        This file is part of the PerSyst Monitoring framework

		Copyright (c) 2010-2016, Leibniz-Rechenzentrum (LRZ), Germany
        See the COPYING file in the base directory of the package for details.
   \endverbatim
 */

#ifndef ERRMSG_H_
#define ERRMSG_H_

#include <string>
#include <iostream>
#include <fstream>
#include <set>
#include <sys/stat.h>
#include "SysTime.h"

using namespace std;

/*!
 *  \addtogroup sw_out
 *  @{
 */
namespace sw_out {

/** simple getter
 * @return debug level
 */
unsigned int getDebugLevel();

/** simple setter
 * @param	dbglvl	[in]	debug level
 */
void setDebugLevel(unsigned int dbglvl);

/** set or concatenate message origin
 * Sets if empty, concatenates if it already contains something
 * @param	pfx	[in]	prefix
 */
void set_msg_originated(const char * pfx);

/** simple setter
 * @param	traceFilename	[in]	trace file name
 */
void setTraceFileName(std::string traceFilename);

/** simple getter
 * @return	get trace file name
 */
std::string getTraceFileName();

void setSilent();

}
/*! @} End of Doxygen Groups*/

/** Log error message
 * @param	fmt	[in]	formatted string
 */
void sw_errmsg(const char *fmt, ...);

/** Log debug message
 * @param	level	[in]	debug level
 * @param	fmt	[in]	formatted string
 */
void sw_dbgmsg(unsigned int level, const char *fmt, ...);

/** Log debug message using string
 * @param	level	[in]	debug level
 * @param	str	[in]	string
 */
void sw_dbgmsgOnlyString(unsigned int level, std::string &str);

/** log memory file for debugging leaks
 * @param	logoutDir	[in]	logout directory
 * @param	msg		[in]	message to log
 */
void mem_out(string &logoutDir, const char* msg);

#ifndef OPEN_MP_FLAG
int omp_get_thread_num();
#endif
#endif /* ERRMSG_H_ */
