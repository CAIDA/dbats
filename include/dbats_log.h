/*
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; either version 2 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with this program; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */

#include <stdarg.h>

#define LOG_ERROR     0
#define LOG_WARNING   1
#define LOG_NORMAL    2
#define LOG_INFO      3

extern int traceLevel;
extern FILE *traceFile;

#define dbats_log(level, ...) dbats_log_func(level, __FILE__, __LINE__, __VA_ARGS__)

extern void dbats_log_func(int level, const char *file, int line, const char * format, ...)
    __attribute__ ((format (printf, 4, 5)));

