#include "headers.h"
#include "logger_module.h"
#include <assert.h>
#include <time.h>
#include <my_global.h>
#include <mysql.h>

#define CODEC_ID 8
#define NUM_OF_DATA 9
#define FISRT_RECORD_OFFSET 10
#define MAX_AVL_RECORDS 50

typedef struct {
	double latitude;
	double longitude;
	int altitude;
	int angle;
	unsigned char satellites;
	int speed;
} gps_element;

typedef struct {
	unsigned char id;
	unsigned long int value;
} io_element_node;

typedef struct {
	unsigned char event_io_id;
	unsigned char number_of_total_io;

	unsigned char number_of_1byte_io;
	io_element_node one_byte_io[5];

	unsigned char number_of_2byte_io;
	io_element_node two_byte_io[5];

	unsigned char number_of_4byte_io;
	io_element_node four_byte_io[4];

	unsigned char number_of_8byte_io;
	io_element_node eight_byte_io[4];

} io_element;

typedef struct {
	long int timestamp;
	unsigned char priority;
	gps_element gps_elem;
	io_element io_elem;
} AVL_data;

typedef struct {
	char imei[50];
	unsigned char codec_id;
	unsigned char number_of_data;
	AVL_data records[MAX_AVL_RECORDS];
} AVL_data_array;

static void parse_gps_element(const unsigned char* data_packet, size_t* pos, gps_element* gps_elem) {
	int int_val, i;
	size_t index = *pos;

	/* Longitude */
	int_val = data_packet[index++];
	for (i = 0; i < 3; i++) {
		int_val <<= 8;
		int_val |= data_packet[index++];
	}
	gps_elem->longitude = int_val / 10000000.0;

	/* Latitude */
	int_val = data_packet[index++];
	for (i = 0; i < 3; i++) {
		int_val <<= 8;
		int_val |= data_packet[index++];
	}
	gps_elem->latitude = int_val / 10000000.0;

	/* Altitude */
	gps_elem->altitude = data_packet[index++];
	gps_elem->altitude <<= 8;
	gps_elem->altitude |= data_packet[index++];

	/* Angle */
	gps_elem->angle = data_packet[index++];
	gps_elem->angle <<= 8;
	gps_elem->angle |= data_packet[index++];

	/* satellites */
	gps_elem->satellites = data_packet[index++];

	/* Speed */
	gps_elem->speed = data_packet[index++];
	gps_elem->speed <<= 8;
	gps_elem->speed |= data_packet[index++];

	*pos = index;
}

/******************************************************************************/
static void parse_io_element(const unsigned char* data_packet, size_t* pos, io_element* io_elem) {
	int i, j;
	size_t index = *pos;

	io_elem->event_io_id = data_packet[index++];
	io_elem->number_of_total_io = data_packet[index++];

	/* 1-byte values */
	io_elem->number_of_1byte_io = data_packet[index++];
	for (i = 0; i < io_elem->number_of_1byte_io; i++) {
		io_elem->one_byte_io[i].id = data_packet[index++];
		io_elem->one_byte_io[i].value = data_packet[index++];
	}

	/* 2-byte values */
	io_elem->number_of_2byte_io = data_packet[index++];
	for (i = 0; i < io_elem->number_of_2byte_io; i++) {
		io_elem->two_byte_io[i].id = data_packet[index++];
		io_elem->two_byte_io[i].value = data_packet[index++];
		io_elem->two_byte_io[i].value <<= 8;
		io_elem->two_byte_io[i].value |= data_packet[index++];
	}

	/* 4-byte values */
	io_elem->number_of_4byte_io = data_packet[index++];
	for (i = 0; i < io_elem->number_of_4byte_io; i++) {
		io_elem->four_byte_io[i].id = data_packet[index++];
		io_elem->four_byte_io[i].value = data_packet[index++];
		for (j = 0; j < 3; j++) {
			io_elem->four_byte_io[i].value <<= 8;
			io_elem->four_byte_io[i].value |= data_packet[index++];
		}
	}

	/* 8-byte values */
	io_elem->number_of_8byte_io = data_packet[index++];
	for (i = 0; i < io_elem->number_of_8byte_io; i++) {
		io_elem->eight_byte_io[i].id = data_packet[index++];
		io_elem->eight_byte_io[i].value = data_packet[index++];
		for (j = 0; j < 7; j++) {
			io_elem->eight_byte_io[i].value <<= 8;
			io_elem->eight_byte_io[i].value |= data_packet[index++];
		}
	}

	*pos = index;
}
/******************************************************************************/

/*  AVL data:  | <timestamp> | <priority> | <GPS element> | <IO Element> |  */
static void parse_AVL_data(const unsigned char* data_packet, size_t* pos, AVL_data* avl_data) {
	int i;
	long int timestamp;
	gps_element gps_elem;
	io_element io_elem;
	size_t index = *pos;

	/* parse timestamp */
	timestamp = data_packet[index++];
	for (i = 0; i < 7; i++) {
		timestamp <<= 8;
		timestamp |= data_packet[index++];
	}
	avl_data->timestamp = timestamp / 1000;
	/* Priority */
	avl_data->priority = data_packet[index++];
	/* GPS Element */
	parse_gps_element(data_packet, &index, &gps_elem);
	avl_data->gps_elem = gps_elem;
	/* IO Element */
	parse_io_element(data_packet, &index, &io_elem);
	avl_data->io_elem = io_elem;

	*pos = index;
}

/******************************************************************************/
void parse_AVL_data_array(const unsigned char* data_packet, AVL_data_array* data_array) {
	size_t position = FISRT_RECORD_OFFSET;
	AVL_data avl_data;
	int i;

	data_array->number_of_data = data_packet[NUM_OF_DATA];
	data_array->codec_id = data_packet[CODEC_ID];

	for (i = 0; i < data_array->number_of_data; i++) {
		parse_AVL_data(data_packet, &position, &avl_data);
		data_array->records[i] = avl_data;
	}
}

/******************************************************************************/

void print_raw_packet(const unsigned char* data_packet, size_t len) {
	int i;
	for (i = 0; i < len; i++)
		printf("%02x|", data_packet[i]);
	printf("\n");
}

void print_AVL_data(const AVL_data_array* data_array) {
	int i, j;
	char buffer[80];
	struct tm* tminfo;
	AVL_data avl_data;

	logger_puts("IMEI: %s", data_array->imei);
	logger_puts("Codec ID: %d", data_array->codec_id);
	logger_puts("Number of data: %d", data_array->number_of_data);

	for (i = 0; i < data_array->number_of_data; i++) {
		logger_puts(" Data %d", i + 1);
		avl_data = data_array->records[i];

		tminfo = localtime(&avl_data.timestamp);
		strftime(buffer, 80, "%Y-%m-%d %H:%M:%S", tminfo);
		logger_puts("   Timestamp: %s", buffer);
		logger_puts("   Priority: %d", avl_data.priority);
		/* GPS element*/
		logger_puts("   GPS Element");
		logger_puts("     Latitude: %lf", avl_data.gps_elem.latitude);
		logger_puts("     Longitude: %lf", avl_data.gps_elem.longitude);
		logger_puts("     Altitude: %d", avl_data.gps_elem.altitude);
		logger_puts("     Angle: %d", avl_data.gps_elem.angle);
		logger_puts("     satellites: %d", avl_data.gps_elem.satellites);
		logger_puts("     Speed: %d", avl_data.gps_elem.speed);
		/* IO Element */
		logger_puts("   IO Element");
		logger_puts("     Event IO ID: %d", avl_data.io_elem.event_io_id);
		logger_puts("     #total IO: %d", avl_data.io_elem.number_of_total_io);
		/* 1-byte */
		logger_puts("     #1-byte IO: %d", avl_data.io_elem.number_of_1byte_io);
		for (j = 0; j < avl_data.io_elem.number_of_1byte_io; j++)
			logger_puts("      (id:%d, val:%lu) ", avl_data.io_elem.one_byte_io[j].id, avl_data.io_elem.one_byte_io[j].value);
		/* 2-byte */
		logger_puts("     #2-byte IO: %d", avl_data.io_elem.number_of_2byte_io);
		for (j = 0; j < avl_data.io_elem.number_of_2byte_io; j++)
			logger_puts("      (id:%d, val:%lu) ", avl_data.io_elem.two_byte_io[j].id, avl_data.io_elem.two_byte_io[j].value);
		/* 4-byte */
		logger_puts("     #4-byte IO: %d", avl_data.io_elem.number_of_4byte_io);
		for (j = 0; j < avl_data.io_elem.number_of_4byte_io; j++)
			logger_puts("      (id:%d, val:%lu) ", avl_data.io_elem.four_byte_io[j].id, avl_data.io_elem.four_byte_io[j].value);
		/* 8-byte */
		logger_puts("     #8-byte IO: %d", avl_data.io_elem.number_of_8byte_io);

		logger_puts("--------------------------------------------\n");
	}
}

/*DB***************************************************************************/
static MYSQL *con;

void db_connect() {
	con = mysql_init(NULL);

	if (con == NULL || mysql_real_connect(con, MYSQL_HOST, MYSQL_USERNAME, MYSQL_PASSWORD, MYSQL_NAME, 0, NULL, 0) == NULL) {
		mysql_close(con);
		logger_puts("Connection failed: %s", mysql_error(con));
		fatal("Connection failed: %s", mysql_error(con));
	} else
		logger_puts("Connected to DB");
	puts("Connected to DB");

}

void db_store_AVL_data_array(const AVL_data_array* data_array) {
	MYSQL_RES *result = NULL;
	int status = 0;
	char query[512];
	char time_str[80];
	struct tm* tminfo;
	int i;
	AVL_data avl_data;

	if (result) {
		mysql_free_result(result);
		result = NULL;
	}

	for (i = 0; i < data_array->number_of_data; i++) {
		avl_data = data_array->records[i];

		tminfo = localtime(&avl_data.timestamp);
		strftime(time_str, 80, "%Y-%m-%d %H:%M:%S", tminfo);

		sprintf(query, "INSERT INTO records(imei, timestamp, latitude, longitude, altitude, angle, satellites, speed) VALUES ('%s', '%s', %lf, %lf, %d, %d, %d, %d)",
				data_array->imei,
				time_str,
				avl_data.gps_elem.latitude,
				avl_data.gps_elem.longitude,
				avl_data.gps_elem.altitude,
				avl_data.gps_elem.angle,
				avl_data.gps_elem.satellites,
				avl_data.gps_elem.speed
				);

		status = mysql_query(con, query);

		if (status) {
			logger_puts("mysql_error, %s\n", mysql_error(con));
			printf("mysql_error, %s\n", mysql_error(con));
			mysql_free_result(result);
			mysql_close(con);
			exit(EXIT_FAILURE);
		}
		mysql_free_result(result);
	}
}

void db_close() {
	if (con)
		mysql_close(con);
}

/*DB***************************************************************************/

/*Clients**********************************************************************/
typedef struct {
	char state;
	GByteArray *imei;
	GByteArray *data_packet;
} client_info;

#define MAXCLIENTS 10000

static int empty_slots[MAXCLIENTS];
static char empty_slot_occupied[MAXCLIENTS];
static int es_index;
static GHashTable* clients_hash;
static client_info clients[MAXCLIENTS];

static void init_empty_slots() {
	for (es_index = 0; es_index < MAXCLIENTS; es_index++) {
		empty_slots[es_index] = es_index;
		empty_slot_occupied[es_index] = 0;
	}
}

static int get_empty_slot() {
	int slot;

	if (es_index == 0)
		fatal("get_empty_slot: empty stack");


	slot = empty_slot_occupied[--es_index];
	if (empty_slot_occupied[slot])
		fatal("get_empty_slot: slot already occupied");

	empty_slot_occupied[es_index] = 1;
	return empty_slots[es_index];
}

static void put_empty_slot(int i) {
	if (es_index >= MAXCLIENTS)
		fatal("put_empty_slot: stack overflow");

	if (!empty_slot_occupied[i])
		return; /* since the slot is already in the pool */

	empty_slots[es_index] = i;
	empty_slot_occupied[es_index] = 0; /* mark as NOT occupied*/
	es_index++;
}

void init_clients_hash() {
	init_empty_slots();
	clients_hash = g_hash_table_new(g_direct_hash, g_direct_equal);
	if (!clients_hash) {
		fatal("ERROR: %s, '%s', line %d, 'g_hash_table_new' failed", __FILE__, __func__, __LINE__);
	}
}

void add_client(struct bufferevent *bev) {
	int empty_slot = get_empty_slot();

	g_hash_table_insert(clients_hash, GINT_TO_POINTER(bev), GINT_TO_POINTER(empty_slot));
	clients[empty_slot].state = WAIT_FOR_IMEI;
	/* allocate memmory */
	clients[empty_slot].imei = g_byte_array_new();
	assert(clients[empty_slot].imei != NULL);
	clients[empty_slot].data_packet = g_byte_array_new();
	assert(clients[empty_slot].data_packet != NULL);
}

client_info* get_client(struct bufferevent *bev) {
	int slot = GPOINTER_TO_INT(g_hash_table_lookup(clients_hash, GINT_TO_POINTER(bev)));

	if (slot < 0 || slot >= MAXCLIENTS) {
		fatal("ERROR: %s, '%s', line %d, slot value (%d) out of range", __FILE__, __func__, __LINE__, slot);
	}

	return &clients[slot];
}

void remove_client(struct bufferevent *bev) {
	int slot = GPOINTER_TO_INT(g_hash_table_lookup(clients_hash, GINT_TO_POINTER(bev)));
	if (slot < 0 || slot >= MAXCLIENTS) {
		fatal("ERROR: %s, '%s', line %d, slot value (%d) out of range", __FILE__, __func__, __LINE__, slot);
	}

	/* free allocated memory */
	g_byte_array_free(clients[slot].imei, TRUE);
	g_byte_array_free(clients[slot].data_packet, TRUE);

	g_hash_table_remove(clients_hash, GINT_TO_POINTER(bev));
	put_empty_slot(slot);
}

/******************************************************************************/

static unsigned char input_buffer[INPUT_BUFSIZE];
struct event_base *base;
static GQueue* queue;
static pthread_mutex_t mtx = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t cond_consumer = PTHREAD_COND_INITIALIZER;
static int max_queue_size;

static void* thread_consumer(void *arg) {
	AVL_data_array *data_array;
	int s;

	db_connect();

	while (1) {
		s = pthread_mutex_lock(&mtx);
		if (s != 0) {
			logger_puts("ERROR: %s, '%s', line %d, pthread_mutex_lock failed with code %d", __FILE__, __func__, __LINE__, s);
			fatal("ERROR: %s, '%s', line %d, pthread_mutex_lock failed with code %d", __FILE__, __func__, __LINE__, s);
		}
		/******* LOCKED *******************************************/

		/* Wait until queue has at least one element*/
		while (queue->length == 0) {
			s = pthread_cond_wait(&cond_consumer, &mtx);
			if (s != 0) {
				logger_puts("ERROR: %s, '%s', line %d, pthread_cond_wait failed with code %d", __FILE__, __func__, __LINE__, s);
				fatal("ERROR: %s, '%s', line %d, pthread_cond_wait failed with code %d", __FILE__, __func__, __LINE__, s);
			}
		}

		/* consume all AVL data*/
		while (queue->length) {
			data_array = g_queue_pop_tail(queue);
			print_AVL_data(data_array);
			db_store_AVL_data_array(data_array);

			printf("IMEI %s, %d data stored id db\n", data_array->imei, data_array->number_of_data);

			free(data_array);
		}

		/******* UNLOCK *******************************************/
		s = pthread_mutex_unlock(&mtx);
		if (s != 0) {
			logger_puts("ERROR: %s, '%s', line %d, pthread_mutex_unlock failed with code %d", __FILE__, __func__, __LINE__, s);
			fatal("ERROR: %s, '%s', line %d, pthread_mutex_unlock failed with code %d", __FILE__, __func__, __LINE__, s);
		}

	}

	return 0;
}

/***********************************************************/

static void push_onto_queue(const client_info* client) {
	AVL_data_array *data_array;
	int s, i;

	data_array = (AVL_data_array*) malloc(sizeof (AVL_data_array));
	if (!data_array) {
		logger_puts("ERROR: %s, '%s', line %d, malloc failed", __FILE__, __func__, __LINE__);
		fatal("ERROR: %s, '%s', line %d, malloc failed", __FILE__, __func__, __LINE__);
	}

	parse_AVL_data_array(client->data_packet->data, data_array);

	for (i = 0; i < client->imei->len - 2; i++)
		data_array->imei[i] = client->imei->data[i + 2];
	data_array->imei[i] = 0;

	s = pthread_mutex_lock(&mtx);
	if (s != 0) {
		logger_puts("ERROR: %s, '%s', line %d, pthread_mutex_lock failed with code %d", __FILE__, __func__, __LINE__, s);
		fatal("ERROR: %s, '%s', line %d, pthread_mutex_lock failed with code %d", __FILE__, __func__, __LINE__, s);
	}

	g_queue_push_head(queue, data_array);

	if (max_queue_size < queue->length) {
		max_queue_size = queue->length;
		logger_puts("Queue max size: %d", max_queue_size);
	}

	print_raw_packet(client->data_packet->data, client->data_packet->len);
	s = pthread_mutex_unlock(&mtx);
	if (s != 0) {
		logger_puts("ERROR: %s, '%s', line %d, pthread_mutex_unlock failed with code %d", __FILE__, __func__, __LINE__, s);
		fatal("%s, '%s', line %d, pthread_mutex_unlock failed with code %d", __FILE__, __func__, __LINE__, s);
	}
}

/***********************************************************/

/* if all bytes of imei are read then return TRUE else return FALSE */
static int process_imei(const unsigned char* data, size_t nbytes, client_info* client) {
	size_t length;
	size_t num_of_read_bytes;

	assert(client != NULL);

	/* append bytes to imei */
	g_byte_array_append(client->imei, (guint8*) data, nbytes);
	num_of_read_bytes = client->imei->len;

	if (num_of_read_bytes > 1) {
		/* more than two bytes have already been read, so we can check
		   whether or not we have read the entire message */
		length = client->imei->data[0];
		length <<= 8;
		length |= client->imei->data[1];

		if ((num_of_read_bytes - 2) < length)
			return FALSE;
		else if ((num_of_read_bytes - 2) == length)
			return TRUE;
		else {
			logger_puts("ERROR: %s, '%s', line %d, number of bytes read is greater than indicated value in the IMEI message (first two bytes)", __FILE__, __func__, __LINE__);
			fatal("ERROR: %s, '%s', line %d, number of bytes read is greater than indicated value in the IMEI message (first two bytes)", __FILE__, __func__, __LINE__);
		}
	}
	return FALSE;
}

/***********************************************************/

/* if all bytes of data packet are read then return TRUE else return FALSE */
static int process_data_packet(const unsigned char* data, size_t nbytes, client_info* client) {
	size_t length;
	size_t num_of_read_bytes;

	g_byte_array_append(client->data_packet, (guint8*) data, nbytes);
	num_of_read_bytes = client->data_packet->len;

	if (num_of_read_bytes > 7) /* if at least 8 bytes are recieved */ {
		length = client->data_packet->data[4];
		length <<= 8;
		length |= client->data_packet->data[5];
		length <<= 8;
		length |= client->data_packet->data[6];
		length <<= 8;
		length |= client->data_packet->data[7];

		if (num_of_read_bytes < (length + 12))
			return FALSE;
		else if (num_of_read_bytes == (length + 12))
			return TRUE;
		else {
			logger_puts("ERROR: %s, '%s', line %d, number of bytes read is greater than indicated value in the data packet (first four bytes)", __FILE__, __func__, __LINE__);
			fatal("ERROR: %s, '%s', line %d, number of bytes read is greater than indicated value in the data packet (first four bytes)", __FILE__, __func__, __LINE__);
		}
	}

	return FALSE;
}

/***********************************************************/

static void serv_event_cb(struct bufferevent *bev, short events, void *ctx) {
	int err;

	if (events & BEV_EVENT_ERROR) {
		err = EVUTIL_SOCKET_ERROR();
		logger_puts("ERROR: %s, '%s', line %d, %s", __FILE__, __func__, __LINE__, evutil_socket_error_to_string(err));
		printf("ERROR: %s, '%s', line %d, %s\n", __FILE__, __func__, __LINE__, evutil_socket_error_to_string(err));
	} else if (events & BEV_EVENT_TIMEOUT) {
		logger_puts("ERROR: %s, '%s', line %d, a timeout expired on the bufferevent", __FILE__, __func__, __LINE__);
		printf("ERROR: %s, '%s', line %d, a timeout expired on the bufferevent\n", __FILE__, __func__, __LINE__);
	}

	if (events & (BEV_EVENT_ERROR | BEV_EVENT_TIMEOUT)) {
		remove_client(bev);
		bufferevent_disable(bev, EV_READ | EV_WRITE);
		bufferevent_setcb(bev, NULL, NULL, NULL, NULL);
		bufferevent_free(bev);
	}

}

/****************************************************************************/

static void serv_read_cb(struct bufferevent *bev, void *ctx) {
	struct evbuffer *input = bufferevent_get_input(bev);
	unsigned char ack[4] = {0, 0, 0, 0};
	int nbytes;
	client_info* client = NULL;

	client = get_client(bev);
	assert(client != NULL);

	if (evbuffer_get_length(input) > INPUT_BUFSIZE) {
		logger_puts("ERROR: %s, '%s', line %d, insufficient buffer size", __FILE__, __func__, __LINE__);
		fatal("ERROR: %s, '%s', line %d, insufficient buffer size", __FILE__, __func__, __LINE__);
	}

	memset(input_buffer, 0, INPUT_BUFSIZE);

	nbytes = bufferevent_read(bev, input_buffer, INPUT_BUFSIZE);
	if (nbytes == -1) {
		logger_puts("ERROR: %s, '%s', line %d, couldn't read data from bufferevent", __FILE__, __func__, __LINE__);
		fatal("ERROR: %s, '%s', line %d, couldn't read data from bufferevent", __FILE__, __func__, __LINE__);
	}

	if (client->state == WAIT_FOR_IMEI) {
		/* if process_imei returns TRUE then imei are read entirely, otherwise stay in the WAIT_FOR_IMEI state*/
		if (process_imei(input_buffer, nbytes, client)) {
			ack[0] = 0x01;
			if (bufferevent_write(bev, ack, 1) == -1) {
				logger_puts("ERROR: %s, '%s', line %d, couldn't write data to bufferevent", __FILE__, __func__, __LINE__);
				fatal("ERROR: %s, '%s', line %d, couldn't write data to bufferevent", __FILE__, __func__, __LINE__);
			}
			client->state = WAIT_00_01_TOBE_SENT;
		}
	} else if (client->state == WAIT_FOR_DATA_PACKET) {
		if (process_data_packet(input_buffer, nbytes, client))/* if entire AVL packet is read*/ {
			/* send #data recieved */
			ack[3] = client->data_packet->data[NUM_OF_DATA];
			if (bufferevent_write(bev, ack, 4) == -1) {
				logger_puts("ERROR: %s, '%s', line %d, couldn't write data to bufferevent", __FILE__, __func__, __LINE__);
				fatal("ERROR: %s, '%s', line %d, couldn't write data to bufferevent", __FILE__, __func__, __LINE__);
			}
			client->state = WAIT_NUM_RECIEVED_DATA_TOBE_SENT;
		}
	}
}

/****************************************************************************/

static void serv_write_cb(struct bufferevent *bev, void *ctx) {
	int s;

	client_info* client = NULL;

	client = get_client(bev);
	assert(client != NULL);

	if (client->state == WAIT_00_01_TOBE_SENT) {
		client->state = WAIT_FOR_DATA_PACKET;
	} else if (client->state == WAIT_NUM_RECIEVED_DATA_TOBE_SENT) {
		push_onto_queue(client); /* for parsing and storing in DB, by another thread */

		/* Wake waiting consumer */
		s = pthread_cond_signal(&cond_consumer);
		if (s != 0) {
			logger_puts("ERROR: %s, '%s', line %d, pthread_cond_signal failed with code %d", __FILE__, __func__, __LINE__, s);
			fatal("ERROR: %s, '%s', line %d, pthread_cond_signal failed with code %d", __FILE__, __func__, __LINE__, s);
		}

		remove_client(bev);
		bufferevent_disable(bev, EV_READ | EV_WRITE);
		bufferevent_setcb(bev, NULL, NULL, NULL, NULL);
		bufferevent_free(bev);
	}
}

/****************************************************************************/

static void accept_conn_cb(struct evconnlistener *listener, evutil_socket_t fd, struct sockaddr *address, int socklen, void *ctx) {
	struct event_base *base = evconnlistener_get_base(listener);
	struct bufferevent *bev = bufferevent_socket_new(base, fd, BEV_OPT_CLOSE_ON_FREE);

	bufferevent_setcb(bev, serv_read_cb, serv_write_cb, serv_event_cb, NULL);

	add_client(bev);

	bufferevent_enable(bev, EV_READ);
}

/****************************************************************************/

static void accept_error_cb(struct evconnlistener *listener, void *ctx) {
	struct event_base *base = evconnlistener_get_base(listener);
	int err = EVUTIL_SOCKET_ERROR();
	logger_puts("Got an error %d (%s) on the listener. Shutting down.\n", err, evutil_socket_error_to_string(err));
	fatal("Got an error %d (%s) on the listener. Shutting down.\n", err, evutil_socket_error_to_string(err));
	event_base_loopexit(base, NULL);
}

int main(int argc, char **argv) {
	struct evconnlistener *listener;
	struct sockaddr_in sin;
	int s, port = PORT;
	void* res;
	pthread_t pthread;

	logger_open("gpsloc.log");

	init_clients_hash();
	queue = g_queue_new();
	assert(queue != NULL);

	/* start parallel thread */
	s = pthread_create(&pthread, NULL, thread_consumer, NULL);
	if (s != 0) {
		logger_puts("ERROR: %s, '%s', line %d, pthread_create failed with error %d", __FILE__, __func__, __LINE__, s);
		fatal("%s, '%s', line %d, pthread_create failed with error %d\n", __FILE__, __func__, __LINE__, s);
	}
	logger_puts("INFO: AVL data consumer started successfully");
	puts("AVL data consumer thread started successfully");


	if (port <= 0 || port > 65535) {
		logger_puts("ERROR: %s, '%s', line %d, invalid port", __FILE__, __func__, __LINE__);
		fatal("ERROR: %s, '%s', line %d, invalid port\n", __FILE__, __func__, __LINE__);
	}

	base = event_base_new();
	if (!base) {
		logger_puts("ERROR: %s, '%s', line %d, couldn't open event base", __FILE__, __func__, __LINE__);
		logger_close();
		fatal("ERROR: %s, '%s', line %d, couldn't open event base\n", __FILE__, __func__, __LINE__);
	}

	/* Clear the sockaddr before using it, in case there are extra
	   platform-specific fields that can mess us up. */
	memset(&sin, 0, sizeof (sin));
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = htonl(0);
	sin.sin_port = htons(port);

	listener = evconnlistener_new_bind(
			base,
			accept_conn_cb,
			NULL, LEV_OPT_CLOSE_ON_FREE | LEV_OPT_REUSEABLE, -1,
			(struct sockaddr*) &sin, sizeof (sin));

	if (!listener) {
		logger_puts("ERROR: %s, '%s', line %d, couldn't create listener", __FILE__, __func__, __LINE__);
		logger_close();
		fatal("ERROR: %s, '%s', line %d, couldn't create listener\n", __FILE__, __func__, __LINE__);
	}

	evconnlistener_set_error_cb(listener, accept_error_cb);
	event_base_dispatch(base);


	/* JUST IN CASE,
	   in fact this code should be unreachable since event_base_dispatch loops infinitely */
	s = pthread_join(pthread, &res);
	if (s != 0) {
		logger_puts("ERROR: %s, '%s', line %d, pthread_join failed with error %d", __FILE__, __func__, __LINE__, s);
		logger_close();
		fatal("ERROR: %s, '%s', line %d, pthread_join failed with error %d\n", __FILE__, __func__, __LINE__, s);
	}
	puts("AVL data consumer thread finished successfully.");

	/* free */
	evconnlistener_free(listener);
	event_base_free(base);

	while (queue->length)
		free(g_queue_pop_head(queue));

	g_queue_free(queue);
	pthread_mutex_destroy(&mtx);
	pthread_cond_destroy(&cond_consumer);

	exit(EXIT_SUCCESS);
}

